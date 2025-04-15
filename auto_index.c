#include "auto_index.h"

PG_MODULE_MAGIC;

planner_hook_type prev_planner_hook = NULL;

char *table_name_glob = 0;
char *col_name_glob = 0;
static void handle_sigterm(int signum){proc_exit(1);}


// Your custom struct
typedef struct {
    int num_queries;
    double benefit;
    double cost;
    int is_indexed;
} MyStruct;

// Inner map: key -> MyStruct
typedef struct InnerMapEntry {
    char key[64];
    MyStruct data;
    UT_hash_handle hh;
} InnerMapEntry;

// Outer map: key -> InnerMapEntry*
typedef struct OuterMapEntry {
    char key[64];
    InnerMapEntry *inner_map; // nested map
    UT_hash_handle hh;
} OuterMapEntry;

OuterMapEntry *outer_map = NULL;

// Add or update entry
void add_entry(const char *outer_key, const char *inner_key, MyStruct value) {
    OuterMapEntry *outer;
    HASH_FIND_STR(outer_map, outer_key, outer);
    if (!outer) {
        outer = malloc(sizeof(OuterMapEntry));
        strcpy(outer->key, outer_key);
        outer->inner_map = NULL;
        HASH_ADD_STR(outer_map, key, outer);
    }

    InnerMapEntry *inner;
    HASH_FIND_STR(outer->inner_map, inner_key, inner);
    if (!inner) {
        inner = malloc(sizeof(InnerMapEntry));
        strcpy(inner->key, inner_key);
        HASH_ADD_STR(outer->inner_map, key, inner);
    }

    inner->data = value;
}

// Get entry
MyStruct *get_entry(const char *outer_key, const char *inner_key) {
    OuterMapEntry *outer;
    HASH_FIND_STR(outer_map, outer_key, outer);
    if (!outer) return NULL;
    if(!inner_key) return outer;

    InnerMapEntry *inner;
    HASH_FIND_STR(outer->inner_map, inner_key, inner);
    return inner ? &inner->data : NULL;
}

// Clean up
void free_all() {
    OuterMapEntry *outer_entry, *outer_tmp;
    HASH_ITER(hh, outer_map, outer_entry, outer_tmp) {
        InnerMapEntry *inner_entry, *inner_tmp;
        HASH_ITER(hh, outer_entry->inner_map, inner_entry, inner_tmp) {
            HASH_DEL(outer_entry->inner_map, inner_entry);
            free(inner_entry);
        }
        HASH_DEL(outer_map, outer_entry);
        free(outer_entry);
    }
}


void delete_entry(const char *outer_key, const char *inner_key) {
    OuterMapEntry *outer;
    HASH_FIND_STR(outer_map, outer_key, outer);
    if (!outer) return;

    InnerMapEntry *inner;
    HASH_FIND_STR(outer->inner_map, inner_key, inner);
    if (inner) {
        HASH_DEL(outer->inner_map, inner);
        free(inner);
    }

    // If inner_map is now empty, delete outer entry too
    if (outer->inner_map == NULL) {
        HASH_DEL(outer_map, outer);
        free(outer);
    }
}

static List *
extract_columns_from_expr(Node *node, List *rtable)
{
    List *colnames = NIL;

    if (node == NULL)
        return NIL;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;
        if (var->varno > 0 && var->varno <= list_length(rtable))
        {
            RangeTblEntry *rte = list_nth_node(RangeTblEntry, rtable, var->varno - 1);
            if (rte->rtekind == RTE_RELATION)
            {
                const char *colname = get_attname(rte->relid, var->varattno, false);
                colnames = lappend(colnames, pstrdup(colname));
            }
        }
    }
    else if (IsA(node, OpExpr))
    {
        OpExpr *op = (OpExpr *) node;
        ListCell *lc;
        foreach(lc, op->args)
        {
            colnames = list_concat(colnames, extract_columns_from_expr((Node *) lfirst(lc), rtable));
        }
    }
    else if (IsA(node, BoolExpr))
    {
        BoolExpr *b = (BoolExpr *) node;
        ListCell *lc;
        foreach(lc, b->args)
        {
            colnames = list_concat(colnames, extract_columns_from_expr((Node *) lfirst(lc), rtable));
        }
    }
    else if (IsA(node, RelabelType))
    {
        RelabelType *r = (RelabelType *) node;
        colnames = list_concat(colnames, extract_columns_from_expr((Node *) r->arg, rtable));
    }

    return colnames;
}

void my_index_info(const char *relname)
{
    Oid relid;
    Relation rel;
    List *indexList;
    ListCell *lc;
    
    // Step 1: Resolve table OID
    relid = RelnameGetRelid(relname);
    if (!OidIsValid(relid))
    {
        elog(LOG, "Relation %s not found", relname);
        return;
    }
    
    // Step 2: Open relation
    rel = relation_open(relid, AccessShareLock);
    if (rel == NULL)
    {
        elog(LOG, "Could not open relation %s", relname);
        return;
    }
    
    // Step 3: Get list of indexes
    indexList = RelationGetIndexList(rel);
    if (indexList == NIL)
    {
        elog(INFO, "Relation %s has no indexes", relname);
        relation_close(rel, AccessShareLock);
        return;
    }
    
    foreach(lc, indexList)
    {
        Oid indexOid = lfirst_oid(lc);
        Relation indexRel = index_open(indexOid, AccessShareLock);

        if (indexRel->rd_index == NULL)
        {
            elog(INFO, "Skipping invalid index");
            index_close(indexRel, AccessShareLock);
            continue;
        }

        Form_pg_index indexForm = indexRel->rd_index;
        int numIndexKeys = indexForm->indnatts;

        for (int i = 0; i < numIndexKeys; i++)
        {
            AttrNumber attnum = indexForm->indkey.values[i];

            if (attnum == 0)
            {
                elog(LOG, "  Column %d is an expression", i + 1);
                continue;
            }

            const char *colname = get_attname(relid, attnum, false);
            add_entry(relname, colname, (MyStruct){0, 0, 0, 1});
            elog(LOG, " Index Column: %s", colname);
        }

        index_close(indexRel, AccessShareLock);
    }

    list_free(indexList);
    relation_close(rel, AccessShareLock);
}


static void find_seqscans(Plan *plan, List *rtable)
{
    if (plan == NULL)
        return;

    if (IsA(plan, SeqScan))
    {
        SeqScan *seq = (SeqScan *) plan;
        Index relid_index = seq->scan.scanrelid;

        RangeTblEntry *rte = list_nth_node(RangeTblEntry, rtable, relid_index - 1);
        const char *table_name = get_rel_name(rte->relid);
        Oid relid = rte->relid;
        Oid nspid = get_rel_namespace(relid);
        const char *nspname = get_namespace_name(nspid);
        char relkind = get_rel_relkind(relid);  // 'r', 'v', 'm', 'i', etc.

        if (!nspname ||
            strcmp(nspname, "pg_catalog") == 0 ||
            strcmp(nspname, "information_schema") == 0 ||
            strncmp(nspname, "pg_toast", 8) == 0 ||
            strncmp(nspname, "pg_temp", 7) == 0 ||
            relkind != RELKIND_RELATION)  // not a regular table
        {
            elog(LOG, "Skipping system or non-user table: %s", get_rel_name(relid));
            return;
        }

        // Optional: check if current user can SELECT from the table
        if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
        {
            elog(LOG, "Skipping table %s: no SELECT permissions", table_name);
            return;
        }


        elog(LOG, "SeqScan on table: %s", table_name);
        elog(LOG, "SeqScan cost: %.2f", plan->startup_cost + plan->total_cost);

        MyStruct* en = get_entry(table_name, NULL);
        if(!en) my_index_info(table_name);
    
        if (plan->qual)
        {
            ListCell *lc;
            foreach(lc, plan->qual)
            {
                Node *qual_node = (Node *) lfirst(lc);
                List *colnames = extract_columns_from_expr(qual_node, rtable);
                ListCell *cell;
                foreach(cell, colnames)
                {
                    char *colname = (char *) lfirst(cell);
                    elog(LOG, "Column used in WHERE clause: %s", colname);

                    MyStruct *entry = get_entry(table_name, colname);
                    if(entry){
                        entry->num_queries++;
                        if(entry->is_indexed == 0){   
                            if(entry->benefit * entry->num_queries > entry->cost){
                                entry->is_indexed = 1;
                                strcpy(table_name_glob, table_name);
                                strcpy(col_name_glob, colname);
                                start_auto_index_worker();
                            }
                        }
                    }
                    else{
                        // num_queries,benefit,cost,is_indexed
                        double n = 0;
                        HeapTuple classTuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rte->relid));
                        if (HeapTupleIsValid(classTuple)) {
                            Form_pg_class classForm = (Form_pg_class) GETSTRUCT(classTuple);
                            n = (double) classForm->reltuples;
                            ReleaseSysCache(classTuple);
                        }
                        
                        MyStruct new_entry = {1, 40, 120, 0};
                        add_entry(table_name, colname, new_entry);
                    }
                }
            }
        }
    }
    // Recurse into child plans
    find_seqscans(plan->lefttree, rtable);
    find_seqscans(plan->righttree, rtable);
}

PGDLLEXPORT void
auto_index_worker_main(Datum main_arg){
    const char *input = MyBgworkerEntry->bgw_extra;

    char table_name[64], column_name[64];
    sscanf(input, "%63[^|]|%63s", table_name, column_name);


	BackgroundWorkerInitializeConnection("postgres", NULL , 0);
    elog(LOG, "WORKER FUNCTION IS RUNNING!!!!");

    pqsignal(SIGTERM, handle_sigterm);
    BackgroundWorkerUnblockSignals();

	StartTransactionCommand();
	PushActiveSnapshot(GetTransactionSnapshot());
    if (SPI_connect() != SPI_OK_CONNECT)
    {
        elog(WARNING, "AutoIndexWorker: SPI_connect failed");
		AbortCurrentTransaction();
        proc_exit(1);
    }

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "select my_index_creator('%s','%s');", table_name, column_name);

    int ret = SPI_execute(query.data, true, 0);
	if(ret != SPI_OK_SELECT){
		elog(WARNING, "AutoIndexWorker: SPI_exec failed for SELECT");
		SPI_finish();
		AbortCurrentTransaction();
		proc_exit(1);
	}

	elog(LOG, "AutoIndexWorker: SPI_exec successful");
	elog(LOG, "AutoIndexWorker: SPI processed %lu rows", SPI_processed);

    SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

    elog(LOG, "AutoIndexWorker: Index creation completed, exiting.");
}


static PlannedStmt *
auto_index_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams){
    elog(LOG, "AutoIndex: Planner hook triggered");

    PlannedStmt *stmt = prev_planner_hook ? 
                        prev_planner_hook(parse, query_string, cursorOptions, boundParams) : 
                        standard_planner(parse, query_string, cursorOptions, boundParams);

	if (stmt){
		find_seqscans(stmt->planTree, stmt->rtable);
	}

    return stmt;
}

void start_auto_index_worker(void) {
    BackgroundWorker worker;
    BackgroundWorkerHandle *handle;
    BgwHandleStatus status;
    pid_t pid;

    memset(&worker, 0, sizeof(worker));
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
    worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    snprintf(worker.bgw_name, BGW_MAXLEN, "AutoIndex Worker");
    sprintf(worker.bgw_library_name, "auto_index");
    sprintf(worker.bgw_function_name, "auto_index_worker_main");

    // Setup bgw_extra
    snprintf(worker.bgw_extra, BGW_EXTRALEN, "%s|%s", table_name_glob, col_name_glob);
    worker.bgw_notify_pid = MyProcPid;

    /* Start the worker process */
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        elog(WARNING, "Failed to start AutoIndex worker");
        return;
    }

    /* Wait for worker to start */
    status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status == BGWH_STARTED) {
        elog(LOG, "AutoIndex Worker started with PID: %d", pid);
    } else {
        elog(WARNING, "AutoIndex Worker failed to start");
    }
}

Datum
auto_index_force_init(PG_FUNCTION_ARGS)
{
    elog(LOG, "auto_index_force_init() was called!");
	if (planner_hook != auto_index_planner_hook){
		prev_planner_hook = planner_hook;
		planner_hook = auto_index_planner_hook;
	}

    table_name_glob = (char*)(malloc (64 * sizeof(char)));
    col_name_glob = (char*)(malloc (64 * sizeof(char)));
    PG_RETURN_VOID();
}

Datum
auto_index_cleanup(PG_FUNCTION_ARGS)
{
    elog(LOG, "AutoIndex: Cleaning up and restoring original planner hook");

    if (planner_hook == auto_index_planner_hook)
        planner_hook = prev_planner_hook;

	if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

	// Statement 1: Drop the event trigger itself
	SPI_execute("DROP EVENT TRIGGER IF EXISTS auto_index_cleanup_tri;", false, 0);

	// Statement 2: Drop the cleanup function
	SPI_execute("DROP FUNCTION IF EXISTS auto_index_cleanup();", false, 0);

	SPI_finish();
    free_all();

    PG_RETURN_VOID();
}

Datum
my_index_creator(PG_FUNCTION_ARGS)
{
    // Get the first argument (table name)
    text *table_text = PG_GETARG_TEXT_PP(0);
    char *table_name = text_to_cstring(table_text);

    // Get the second argument (column name)
    text *col_text = PG_GETARG_TEXT_PP(1);
    char *col_name = text_to_cstring(col_text);

    if(table_name == NULL || col_name == NULL){
        elog(ERROR, "Table or column name is NULL");
        proc_exit(1);
    }

    elog(LOG, "Creating index on %s.%s", table_name, col_name);
    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "CREATE INDEX IF NOT EXISTS my_index_%s_%s ON %s (%s);",
        table_name, col_name, table_name, col_name);

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    int ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY)
        elog(ERROR, "Index creation failed");

    SPI_finish();
    PG_RETURN_VOID();
}
#include "auto_index.h"

PG_MODULE_MAGIC;

planner_hook_type prev_planner_hook = NULL;

static void handle_sigterm(int signum){proc_exit(1);}
static bool in_hook = false;
bool auto_index_enabled = true;

void get_table_page_tuple_count(const char *table_name, int* page_count, int* tuple_count)
{
    Oid rel_oid;
    HeapTuple tuple;
    Form_pg_class classForm;

    // Get OID of the relation (table)
    rel_oid = RelnameGetRelid(table_name);

    if (!OidIsValid(rel_oid))
    {
        elog(ERROR, "Invalid table name: %s", table_name);
        return;
    }

    // Fetch tuple from pg_class using syscache
    tuple = SearchSysCache1(RELOID, ObjectIdGetDatum(rel_oid));

    if (!HeapTupleIsValid(tuple))
    {
        elog(ERROR, "Could not find pg_class tuple for table: %s", table_name);
        return;
    }

    // Cast the tuple to Form_pg_class to access fields
    classForm = (Form_pg_class) GETSTRUCT(tuple);

    *page_count = classForm->relpages;
    *tuple_count = classForm->reltuples;

    // Release the syscache
    ReleaseSysCache(tuple);
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
                // l stands for list 
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

bool my_index_info(const char *relname, const char* target){
    bool ans = false;
    Oid relid;
    Relation rel;
    List *indexList;
    ListCell *lc;
    
    // Resolve table OID
    relid = RelnameGetRelid(relname);
    if (!OidIsValid(relid))
    {
        elog(LOG, "Relation %s not found", relname);
        return;
    }
    
    // Open relation
    rel = relation_open(relid, AccessShareLock);
    if (rel == NULL)
    {
        elog(LOG, "Could not open relation %s", relname);
        return;
    }
    
    // Get list of indexes
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

        for (int i = 0; i < numIndexKeys; i++)        {
            AttrNumber attnum = indexForm->indkey.values[i];

            if (attnum == 0)
            {
                elog(LOG, "  Column %d is an expression", i + 1);
                continue;
            }

            const char *colname = get_attname(relid, attnum, false);
            elog(LOG, " Index Column: %s", colname);
            ans = (strcmp(colname,target)==0)?true:false;
            if(ans) break;
        }
        index_close(indexRel, AccessShareLock);
        if(ans) break;
    }
    list_free(indexList);
    relation_close(rel, AccessShareLock);
    return ans;
}

static void find_seqscans(Plan *plan, List *rtable){
    if (plan == NULL)
        return;

    if (IsA(plan, SeqScan))
    {
        SeqScan *seq = (SeqScan *) plan;
        Index relid_index = seq->scan.scanrelid;

        RangeTblEntry *rte = list_nth_node(RangeTblEntry, rtable, relid_index - 1);
        const char *table_name = get_rel_name(rte->relid);
        if(strcmp(table_name,"aidx_queries")==0){
            elog(LOG, "Skipping aidx_queries table");
            return;
        }
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

        // Check if current user can SELECT from the table
        if (pg_class_aclcheck(relid, GetUserId(), ACL_SELECT) != ACLCHECK_OK)
        {
            elog(LOG, "Skipping table %s: no SELECT permissions", table_name);
            return;
        }

        elog(LOG, "SeqScan on table: %s", table_name);
        elog(LOG, "SeqScan cost: %.2f", plan->startup_cost + plan->total_cost);
    
        // if WHERE clause exists
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

                    char *query = palloc(BUFFER_SIZE);
                    snprintf(query, BUFFER_SIZE,
                        "SELECT tablename, colname, cost, benefit, num_queries, is_indexed "
                        "FROM aidx_queries WHERE tablename = '%s' AND colname = '%s'",
                        table_name, colname
                    );

                    int ret = SPI_connect();
                    if (ret != SPI_OK_CONNECT)
                    elog(ERROR, "SPI_connect failed: error code %d", ret);
                    
                    ret = SPI_execute(query, true, 0);  // read-only, unlimited rows
                    if (ret != SPI_OK_SELECT)
                        elog(ERROR, "SPI_execute failed: error code %d", ret);
                    pfree(query);
                        
                    int proc = SPI_processed;
                    elog(LOG, "Rows returned: %d", proc);
                        
                    if(proc == 1){
                        HeapTuple tuple = SPI_tuptable->vals[0];
                        TupleDesc tupdesc = SPI_tuptable->tupdesc;
                        bool isnull;
                        
                        // Column 1: tablename (text)
                        Datum tablename_datum = SPI_getbinval(tuple, tupdesc, 1, &isnull);
                        char *tablename = isnull ? "<null>" : TextDatumGetCString(tablename_datum);

                        // Column 2: colname (text)
                        Datum colname_datum = SPI_getbinval(tuple, tupdesc, 2, &isnull);
                        char *colname = isnull ? "<null>" : TextDatumGetCString(colname_datum);
                        
                        // Column 3: cost (float8)
                        // Datum cost_datum = SPI_getbinval(tuple, tupdesc, 3, &isnull);
                        // double cost = isnull ? 0.0 : DatumGetFloat8(cost_datum);
                        
                        // Column 4: benefit (float8)
                        // Datum benefit_datum = SPI_getbinval(tuple, tupdesc, 4, &isnull);
                        // double benefit = isnull ? 0.0 : DatumGetFloat8(benefit_datum);
                        
                        // Column 5: num_queries (int)
                        Datum num_q_datum = SPI_getbinval(tuple, tupdesc, 5, &isnull);
                        int32 num_queries = isnull ? 0 : DatumGetInt32(num_q_datum);

                        // Column 6: is_indexed (bool)
                        // Datum indexed_datum = SPI_getbinval(tuple, tupdesc, 6, &isnull);
                        // bool is_indexed = isnull ? false : DatumGetBool(indexed_datum);

                        int page_count = 0, tuple_count = 0;
                        get_table_page_tuple_count(table_name, &page_count, &tuple_count);
                        bool is_indexed = my_index_info(table_name, colname);
                        
                        double cost = tuple_count;
                        if(cost<=0) cost = 100;
                        int height = ceil(log(cost)/log(100));
                        double benefit = page_count - height;
                        if(benefit<=0) benefit = 1;

                        // Log it
                        elog(LOG, "Row %d: table=%s, column=%s, cost=%.2f, benefit=%.2f, queries=%d, indexed=%s",
                            0, tablename, colname, cost, benefit, num_queries, is_indexed ? "true" : "false");

                        if(!is_indexed){
                            if((num_queries+1) * benefit > cost){
                                is_indexed = true;
                                snprintf(query, BUFFER_SIZE,
                                    "create index if not exists my_index_%s_%s on %s (%s);",
                                    tablename, colname, tablename, colname
                                );
                                start_auto_index_worker(query,false);
                            }
                        }
                        
                        if(is_indexed){
                            snprintf(query, BUFFER_SIZE,
                                "UPDATE aidx_queries SET num_queries = num_queries + 1,cost=%.2f, benefit=%.2f, is_indexed = 't' WHERE tablename = '%s' AND colname = '%s'",cost,benefit,table_name, colname
                            );
                        }
                        else{
                            snprintf(query, BUFFER_SIZE,
                                "UPDATE aidx_queries SET num_queries = num_queries + 1,cost=%.2f, benefit=%.2f, is_indexed = 'f' WHERE tablename = '%s' AND colname = '%s'",cost,benefit,table_name, colname
                            );
                        }
                        
                    } else if (proc == 0){
                        bool ans = my_index_info(table_name, colname);
                        int page_count = 0, tuple_count = 0;
                        get_table_page_tuple_count(table_name, &page_count, &tuple_count);

                        double cost = tuple_count;
                        if(cost<=0) cost = 100;
                        int height = ceil(log(cost)/log(100));
                        double benefit = page_count - height;
                        if(benefit<=0) benefit = 1;
            
                        if(!ans){
                            snprintf(query, BUFFER_SIZE,
                                "INSERT INTO aidx_queries values('%s', '%s', %.2f, %.2f, 1, 'f') ON CONFLICT (tablename, colname) DO UPDATE SET num_queries = aidx_queries.num_queries + 1",
                                table_name, colname, cost, benefit
                            );
                        }
                        else{
                            snprintf(query, BUFFER_SIZE,
                                "INSERT INTO aidx_queries values('%s', '%s', %.2f, %.2f, 1, 't') ON CONFLICT (tablename, colname) DO UPDATE SET num_queries = aidx_queries.num_queries + 1",
                                table_name, colname, cost, benefit
                            );
                        }
                    }
                    
                    SPI_execute(query, false, 0);
                    SPI_finish();
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
    const char *query = MyBgworkerEntry->bgw_extra;
    elog(LOG, "INSIDE WORKER's MAIN FUNCTION with query: %s!!!!",query);
	BackgroundWorkerInitializeConnection("postgres", NULL , 0);

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

    int ret = SPI_execute(query, false, 0);
	if(ret < 0){
		elog(WARNING, "AutoIndexWorker: SPI_exec failed in worker main");
		SPI_finish();
		AbortCurrentTransaction();
		proc_exit(1);
	}
    else{
        elog(LOG, "AutoIndexWorker: SPI_exec successful");
        elog(LOG, "AutoIndexWorker: SPI processed %lu rows", SPI_processed);
    }


    SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

    elog(LOG, "AutoIndexWorker: Index creation completed, exiting.");
}


static PlannedStmt *
auto_index_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams){
    PlannedStmt *stmt = prev_planner_hook ? 
    prev_planner_hook(parse, query_string, cursorOptions, boundParams) : 
    standard_planner(parse, query_string, cursorOptions, boundParams);

	if (stmt && !in_hook && auto_index_enabled){
        elog(LOG, "AutoIndex: Planner hook triggered");
        in_hook = true;
		find_seqscans(stmt->planTree, stmt->rtable);
        in_hook = false;
	}

    return stmt;
}

void start_auto_index_worker(char* query, bool wait_to_finish) {
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
    snprintf(worker.bgw_extra, BGW_EXTRALEN, "%s", query);
    worker.bgw_notify_pid = MyProcPid;
    pfree(query);

    // Start the worker process 
    if (!RegisterDynamicBackgroundWorker(&worker, &handle)) {
        elog(WARNING, "Failed to start AutoIndex worker");
        return;
    }

    // Wait for worker to start 
    status = WaitForBackgroundWorkerStartup(handle, &pid);
    if (status == BGWH_STARTED) {
        elog(LOG, "AutoIndex Worker started with PID: %d", pid);
    } else {
        elog(WARNING, "AutoIndex Worker failed to start");
    }

    if(wait_to_finish){
        status = WaitForBackgroundWorkerShutdown(handle);
        if(status == BGWH_STOPPED) {
            elog(LOG, "AutoIndex Worker stopped");
        } else {
            elog(WARNING, "AutoIndex Worker did not stop as expected");
        }
    }
}

void _PG_init(void)
{
    elog(LOG, "PG init() was called at PID: %d", MyProcPid);
    DefineCustomBoolVariable(
        "auto_index.enable",                                    // name shown to user
        "Enable or disable automatic indexing logic",           // description
        NULL,                                                   // no short description
        &auto_index_enabled,                                    // pointer to the variable
        true,                                                   // default value
        PGC_SUSET,                                              // can be set by superuser (can also use PGC_USERSET)
        0,                                                      // GUC flags
        NULL, NULL, NULL                                        // no hooks
    );

	if (planner_hook != auto_index_planner_hook){
		prev_planner_hook = planner_hook;
		planner_hook = auto_index_planner_hook;
	}
}

Datum
auto_index_cleanup(PG_FUNCTION_ARGS)
{
    elog(LOG, "AutoIndex: Cleaning up and restoring original planner hook");

    auto_index_enabled = false;
    if (planner_hook == auto_index_planner_hook)
        planner_hook = prev_planner_hook;

	if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

	// Statement 1: Drop the event trigger itself
	SPI_execute("DROP EVENT TRIGGER IF EXISTS auto_index_cleanup_tri;", false, 0);

	// Statement 2: Drop the cleanup function
	SPI_execute("DROP FUNCTION IF EXISTS auto_index_cleanup();", false, 0);

	SPI_finish();

    PG_RETURN_VOID();
}

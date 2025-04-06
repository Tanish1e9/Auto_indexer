#include "auto_index.h"

PG_MODULE_MAGIC;

planner_hook_type prev_planner_hook = NULL;

static void handle_sigterm(int signum){proc_exit(1);}

double extract_total_cost_from_json(const char *json)
{
    Jsonb *jb;
    JsonbIterator *it;
    JsonbValue v;
    JsonbValue key;
    JsonbValue val;
    double total_cost = -1.0;

    // Convert string to Jsonb
    Datum json_datum = DirectFunctionCall1(jsonb_in, CStringGetDatum(json));
    jb = DatumGetJsonbP(json_datum);

    it = JsonbIteratorInit(&jb->root);

    JsonbIteratorToken r;
    bool in_plan = false;

    while ((r = JsonbIteratorNext(&it, &v, true)) != WJB_DONE)
    {
        if (r == WJB_KEY)
        {
            if (v.type == jbvString && strcmp(v.val.string.val, "Plan") == 0)
            {
                in_plan = true;
            }
            else if (in_plan && v.type == jbvString && strcmp(v.val.string.val, "Total Cost") == 0)
            {
                // Next value will be the actual cost
                r = JsonbIteratorNext(&it, &val, true);
                if (val.type == jbvNumeric)
                {
                    total_cost = DatumGetFloat8(DirectFunctionCall1(numeric_float8, NumericGetDatum(val.val.numeric)));
                    break;
                }
            }
        }
    }

    return total_cost;
}



double estimate_index_creation_cost(const char *table, const char *column)
{
    StringInfoData query;
    SPI_connect();
    initStringInfo(&query);

    appendStringInfo(&query,
        "EXPLAIN (FORMAT JSON) CREATE INDEX idx_temp_%s_%s ON %s(%s);",
        table, column, table, column);

    int ret = SPI_execute(query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        elog(WARNING, "Failed to EXPLAIN CREATE INDEX");
        SPI_finish();
        return -1.0;
    }

    char *json_str = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
                                                       SPI_tuptable->tupdesc,
                                                       1,
                                                       NULL));

    double cost = extract_total_cost_from_json(json_str);

    SPI_finish();
    return cost;
}

double estimate_index_benefit(const char *table, const char *column, const char *value)
{
    StringInfoData base_query, index_query;
    SPI_connect();

    initStringInfo(&base_query);
    initStringInfo(&index_query);

    appendStringInfo(&base_query,
        "EXPLAIN (FORMAT JSON) SELECT * FROM %s WHERE %s = '%s';",
        table, column, value);

    appendStringInfo(&index_query,
        "SET enable_seqscan = OFF;");

    // Get plan cost *with* index
    SPI_execute(index_query.data, false, 0);
    int ret = SPI_execute(base_query.data, true, 0);
    if (ret != SPI_OK_SELECT || SPI_processed == 0)
    {
        elog(WARNING, "Failed to EXPLAIN SELECT with index");
        SPI_finish();
        return -1.0;
    }

    char *json_str_index = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
                                                             SPI_tuptable->tupdesc,
                                                             1,
                                                             NULL));
    double cost_with_index = extract_total_cost_from_json(json_str_index);

    // Re-enable seqscan
    SPI_execute("SET enable_seqscan = ON;", false, 0);

    // Get plan cost *without* index
    SPI_execute(base_query.data, true, 0);
    char *json_str_seq = TextDatumGetCString(SPI_getbinval(SPI_tuptable->vals[0],
                                                           SPI_tuptable->tupdesc,
                                                           1,
                                                           NULL));
    double cost_without_index = extract_total_cost_from_json(json_str_seq);

    SPI_finish();

    double benefit = cost_without_index - cost_with_index;
    return benefit > 0 ? benefit : 0.0;
}



static List *extract_columns_from_expr(Node *node, List *rtable)
{
    List *colnames = NIL;

    if (node == NULL)
        return colnames;

    if (IsA(node, Var))
    {
        Var *var = (Var *) node;

        if (var->varno > 0 && var->varno <= list_length(rtable))
        {
            RangeTblEntry *rte = list_nth_node(RangeTblEntry, rtable, var->varno - 1);
            if (rte->rtekind == RTE_RELATION)
            {
                const char *colname = get_attname(rte->relid, var->varattno, false);
                const char *tablename = get_rel_name(rte->relid);

                elog(LOG, "Referenced column: %s.%s", tablename, colname);

                // Avoid duplicates
                bool found = false;
                ListCell *lc;
                foreach(lc, colnames)
                {
                    if (strcmp(colname, (char *) lfirst(lc)) == 0)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
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
            List *subcols = extract_columns_from_expr((Node *) lfirst(lc), rtable);
            colnames = list_concat_unique(colnames, subcols);
        }
    }
    else if (IsA(node, BoolExpr))
    {
        BoolExpr *b = (BoolExpr *) node;
        ListCell *lc;
        foreach(lc, b->args)
        {
            List *subcols = extract_columns_from_expr((Node *) lfirst(lc), rtable);
            colnames = list_concat_unique(colnames, subcols);
        }
    }
    else if (IsA(node, RelabelType))
    {
        RelabelType *relab = (RelabelType *) node;
        List *subcols = extract_columns_from_expr((Node *) relab->arg, rtable);
        colnames = list_concat_unique(colnames, subcols);
    }

    return colnames;
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

        elog(LOG, "SeqScan on table: %s", table_name);
        elog(LOG, "SeqScan cost: %.2f", plan->startup_cost + plan->total_cost);

        if (plan->qual)
        {
            ListCell *lc;
            foreach(lc, plan->qual)
            {
                Node *qual_node = (Node *) lfirst(lc);
                List *colnames = extract_columns_from_expr(qual_node, rtable);

                // ListCell *cell;
                // foreach(cell, colnames)
                // {
                //     char *colname = (char *) lfirst(cell);

                //     // Use a representative value for estimation
                //     const char *sample_value = "123";  // You could make this smarter

                //     double creation_cost = estimate_index_creation_cost(table_name, colname);
                //     double benefit = estimate_index_benefit(table_name, colname, sample_value);

                //     elog(LOG, "Column: %s | Index Creation Cost: %.2f | Benefit: %.2f",
                //          colname, creation_cost, benefit);
                // }
            }
        }
    }

    // Recurse into child plans
    find_seqscans(plan->lefttree, rtable);
    find_seqscans(plan->righttree, rtable);
}

PGDLLEXPORT void
auto_index_worker_main(Datum main_arg)
{
	BackgroundWorkerInitializeConnection("postgres", NULL , 0);
    elog(LOG, "WORKER CHAL RHA H !!!!");

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

	int ret = SPI_execute("select my_index_creator('stud','id');", true, 0);
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

    PG_RETURN_VOID();
}

Datum
my_index_creator(PG_FUNCTION_ARGS)
{
    text *table_text = PG_GETARG_TEXT_P(0);
    text *column_text = PG_GETARG_TEXT_P(1);

    char *table_name = text_to_cstring(table_text);
    char *col_name = text_to_cstring(column_text);

    StringInfoData query;
    initStringInfo(&query);
    appendStringInfo(&query,
        "CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s);",
        table_name, col_name, table_name, col_name);

    if (SPI_connect() != SPI_OK_CONNECT)
        elog(ERROR, "SPI_connect failed");

    int ret = SPI_execute(query.data, false, 0);
    if (ret != SPI_OK_UTILITY)
        elog(ERROR, "Index creation failed");

    SPI_finish();
    PG_RETURN_VOID();
}
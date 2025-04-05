#include "auto_index.h"

PG_MODULE_MAGIC;

planner_hook_type prev_planner_hook = NULL;
bool got_sigterm = false, inside_hook = false;

static void handle_sigterm(int signum)
{
    got_sigterm = true;
    proc_exit(1);
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

	int ret = SPI_execute("select 8;", true, 0);
	if(ret != SPI_OK_SELECT){
		elog(WARNING, "AutoIndexWorker: SPI_exec failed for SELECT");
		SPI_finish();
		AbortCurrentTransaction();
		proc_exit(1);
	}

	elog(LOG, "AutoIndexWorker: SPI_exec successful");
	elog(LOG, "AutoIndexWorker: SPI processed %lu rows", SPI_processed);

    // /* Fetch queries needing an index */
    // const char *query = "SELECT tablename, colname FROM aidx_queries WHERE benefit * num_queries > cost;";
    // ret = SPI_execute(query, true, 0);

    // if (ret != SPI_OK_SELECT)
    // {
    //     elog(WARNING, "AutoIndexWorker: SPI_exec failed for SELECT");
    //     SPI_finish();
    //     proc_exit(1);
    // }

    // for (int i = 0; i < SPI_processed; i++)
    // {
    //     char *tablename = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 1);
    //     char *colname = SPI_getvalue(SPI_tuptable->vals[i], SPI_tuptable->tupdesc, 2);

    //     if (tablename && colname)
    //     {
    //         /* Create Index */
    //         StringInfoData create_index;
    //         initStringInfo(&create_index);
    //         appendStringInfo(&create_index,
    //             "CREATE INDEX IF NOT EXISTS idx_%s_%s ON %s (%s);",
    //             tablename, colname, tablename, colname);
            
    //         elog(LOG, "AutoIndexWorker: Creating index: %s", create_index.data);
    //         SPI_execute(create_index.data, false, 0);

    //         /* Remove from aidx_queries after index creation */
    //         StringInfoData delete_entry;
    //         initStringInfo(&delete_entry);
    //         appendStringInfo(&delete_entry,
    //             "DELETE FROM aidx_queries WHERE tablename = '%s' AND colname = '%s';",
    //             tablename, colname);

    //         SPI_execute(delete_entry.data, false, 0);

    //         /* Free memory */
    //         pfree(create_index.data);
    //         pfree(delete_entry.data);
    //     }
    // }

    SPI_finish();
	PopActiveSnapshot();
	CommitTransactionCommand();

    elog(LOG, "AutoIndexWorker: Index creation completed, exiting.");
}


static PlannedStmt *
auto_index_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams)
{
    elog(LOG, "AutoIndex: Planner hook triggered");

    if (parse->commandType == CMD_SELECT)
    {
        elog(LOG, "AutoIndex: Processing SELECT query...");
        start_auto_index_worker();
        // Get tables
        ListCell *lc;
        foreach (lc, parse->rtable)
        {
            RangeTblEntry *rte = (RangeTblEntry *) lfirst(lc);
            if (rte->rtekind == RTE_RELATION)
            {
                elog(LOG, "AutoIndex: Table: %s", get_rel_name(rte->relid));
            }
        }

        // // Get selected attributes
        // foreach (lc, parse->targetList)
        // {
        //     TargetEntry *tle = (TargetEntry *) lfirst(lc);
        //     if (tle->resjunk) continue; // Skip system columns
            
        //     if (IsA(tle->expr, Var))
        //     {
        //         Var *var = (Var *) tle->expr;

        //         /* Get the corresponding RangeTblEntry */
        //         RangeTblEntry *rte = (RangeTblEntry *) list_nth(parse->rtable, var->varno - 1);
                
        //         if (rte->rtekind == RTE_RELATION) // Ensure it's a real table
        //         {
        //             const char *colname = get_attname(rte->relid, var->varattno, false);
        //             elog(LOG, "AutoIndex: Table: %s, Column: %s", rte->eref->aliasname, colname);
        //         }
        //     }
        // }

        // // Get WHERE conditions
        // if (parse->jointree && parse->jointree->quals)
        // {
        //     Node *whereClause = parse->jointree->quals;
        //     elog(LOG, "AutoIndex: WHERE clause detected!");

        //     if (IsA(whereClause, OpExpr))
        //     {
        //         OpExpr *op = (OpExpr *) whereClause;
        //         elog(LOG, "AutoIndex: Operator used: %s", get_opname(op->opno));
        //     }
        // }
    }

    PlannedStmt *stmt = prev_planner_hook ? 
                        prev_planner_hook(parse, query_string, cursorOptions, boundParams) : 
                        standard_planner(parse, query_string, cursorOptions, boundParams);

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
#include "auto_index.h"

PG_MODULE_MAGIC;

planner_hook_type prev_planner_hook = NULL;

static void handle_sigterm(int signum){proc_exit(1);}

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

	int ret = SPI_execute("select my_index_creator();", true, 0);
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
            switch (rte->rtekind)
            {
                case RTE_RELATION:
                    elog(LOG, "AutoIndex: Table: %s (relid = %u)", get_rel_name(rte->relid), rte->relid);
                    break;

                case RTE_SUBQUERY:
                    elog(LOG, "AutoIndex: Found subquery");
                    log_rte_tables(rte->subquery); // recursive inspection
                    break;

                default:
                    elog(LOG, "AutoIndex: Unhandled RTE kind: %d", rte->rtekind);
                    break;
            }
        }
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
#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "postmaster/interrupt.h"
#include "storage/lwlock.h"

/* these headers are used by this particular worker's code */
#include "access/xact.h"
#include "pgstat.h"
#include "utils/acl.h"
#include "utils/snapmgr.h"

#include "fmgr.h"

/* Background worker + process control */
#include "miscadmin.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/proc.h"
#include "postmaster/bgworker.h"

/* SPI for executing SQL */
#include "executor/spi.h"
#include "commands/dbcommands.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "tcop/utility.h"

/* StringInfo */
#include "lib/stringinfo.h"
#include "optimizer/planner.h"

extern planner_hook_type prev_planner_hook;

static PlannedStmt *auto_index_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);

PGDLLEXPORT void auto_index_worker_main(Datum main_arg);
void start_auto_index_worker(void);

PG_FUNCTION_INFO_V1(auto_index_force_init);
PG_FUNCTION_INFO_V1(auto_index_cleanup);

#endif
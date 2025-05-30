#ifndef AUTO_INDEX_H
#define AUTO_INDEX_H

#include "postgres.h"
#include "utils/fmgroids.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/namespace.h"
#include "utils/relcache.h"
#include "utils/typcache.h"
#include "catalog/index.h"
#include "access/heapam.h"
#include "utils/syscache.h"

#include "postmaster/interrupt.h"
#include "storage/lwlock.h"
#include "utils/json.h"
#include "utils/jsonb.h"

#include "nodes/execnodes.h"
#include "nodes/plannodes.h"
#include "parser/parsetree.h"
#include "utils/rel.h"

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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "utils/guc.h"
#include <math.h>

#define BUFFER_SIZE 1024

extern planner_hook_type prev_planner_hook;
static PlannedStmt *auto_index_planner_hook(Query *parse, const char *query_string, int cursorOptions, ParamListInfo boundParams);
PGDLLEXPORT void auto_index_worker_main(Datum main_arg);
void start_auto_index_worker(char* query, bool wait_to_finish);
PG_FUNCTION_INFO_V1(auto_index_cleanup);


#endif
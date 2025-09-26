# QueryDesc

## Location
src/include/executor/execdesc.h: 33 - 56

## Overview
QueryDesc is a central data structure that encapsulates everything the PostgreSQL executor needs to execute a query, serving as a complete descriptor containing query metadata, execution state, and result handling information.

## Definition

```c
typedef struct QueryDesc
{
	/* These fields are provided by CreateQueryDesc */
	CmdType		operation;		/* CMD_SELECT, CMD_UPDATE, etc. */
	PlannedStmt *plannedstmt;	/* planner's output (could be utility, too) */
	const char *sourceText;		/* source text of the query */
	Snapshot	snapshot;		/* snapshot to use for query */
	Snapshot	crosscheck_snapshot;	/* crosscheck for RI update/delete */
	DestReceiver *dest;			/* the destination for tuple output */
	ParamListInfo params;		/* param values being passed in */
	QueryEnvironment *queryEnv; /* query environment passed in */
	int			instrument_options; /* OR of InstrumentOption flags */

	/* These fields are set by ExecutorStart */
	TupleDesc	tupDesc;		/* descriptor for result tuples */
	EState	   *estate;			/* executor's query-wide state */
	PlanState  *planstate;		/* tree of per-plan-node state */

	/* This field is set by ExecutePlan */
	bool		already_executed;	/* true if previously executed */

	/* This is always set NULL by the core system, but plugins can change it */
	struct Instrumentation *totaltime;	/* total time spent in ExecutorRun */
} QueryDesc;
```
## Detailed Description
QueryDesc serves as the primary interface between PostgreSQL's query planner and executor. It contains all necessary information for query execution including the planned statement, execution parameters, snapshot information for transaction isolation, and execution state. The structure is designed to support both regular SQL queries and utility statements, though utility statements must not be passed to the executor.

The QueryDesc lifecycle follows a typical pattern: it's created by CreateQueryDesc with initial query information, populated with execution state during ExecutorStart, used during query execution via ExecutePlan, and finally cleaned up with FreeQueryDesc. The structure supports query instrumentation and performance monitoring through the totaltime field, which can be utilized by plugins for execution time tracking.

## Parameters / Member Variables
- : Specifies the type of SQL command (CMD_SELECT, CMD_UPDATE, CMD_INSERT, CMD_DELETE, etc.)
- : Points to the output from the query planner, containing the optimized execution plan
- : Contains the original SQL query text as submitted by the client
- : Transaction snapshot used for query execution to ensure proper isolation
- : Additional snapshot used for referential integrity checks during updates/deletes
- : Destination receiver that handles where query results should be sent
- : Parameter list containing values for parameterized queries (prepared statements)
- : Query environment containing additional context and ephemeral relations
- : Bitmask of instrumentation flags controlling performance monitoring
- : Tuple descriptor defining the structure of result tuples (set during ExecutorStart)
- : Executor state containing query-wide execution information and context
- : Root of the plan state tree representing per-node execution state
- : Flag indicating whether the query has been executed previously
- : Instrumentation data tracking total execution time (extensible by plugins)

## Dependencies
- Functions called/Symbols referenced:
  - CmdType
  - PlannedStmt
  - DestReceiver
  - ParamListInfo
  - QueryEnvironment
  - Instrumentation
- Called from (representative examples):
  - CreateQueryDesc (src/backend/tcop/pquery.c:76)
  - ExecutorStart (src/backend/executor/execMain.c:121)
  - ExecutorRun (src/backend/executor/execMain.c:299)
  - ExecutorFinish (src/backend/executor/execMain.c:400)
  - ExecutorEnd (src/backend/executor/execMain.c:460)
  - ProcessQuery (src/backend/tcop/pquery.c:143)
  - PortalStart (src/backend/tcop/pquery.c:440)

## Notes and Other Information
- QueryDesc supports both regular SQL queries and utility statements, but utility statements must not be passed to the executor
- The structure is designed to be extensible, particularly through the totaltime field which plugins can modify
- Fields are populated in phases: initial fields by CreateQueryDesc, execution fields by ExecutorStart, and execution flags by ExecutePlan
- The already_executed flag enables portal rewind functionality for scrollable cursors
- QueryDesc is fundamental to PostgreSQL's executor architecture and is used throughout the query execution pipeline
- The structure integrates with PostgreSQL's instrumentation framework for performance monitoring and query analysis
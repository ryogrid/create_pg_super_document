# CreateQueryDesc

## Location
[src/backend/tcop/pquery.c:67-104](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/tcop/pquery.c#L67-L104)

## Overview
CreateQueryDesc constructs and initializes a QueryDesc structure that encapsulates all information needed to execute a planned query statement in PostgreSQL.

## Definition

```c
QueryDesc *
CreateQueryDesc(PlannedStmt *plannedstmt,
				const char *sourceText,
				Snapshot snapshot,
				Snapshot crosscheck_snapshot,
				DestReceiver *dest,
				ParamListInfo params,
				QueryEnvironment *queryEnv,
				int instrument_options)
```
## Detailed Description
CreateQueryDesc allocates and populates a QueryDesc structure with all the essential components required for query execution. It serves as a constructor function that packages the planned statement, execution context, parameters, and output destination into a single descriptor object. The function registers the provided snapshots to ensure proper transaction isolation and sets up initial state for query execution. Fields related to execution state (tupDesc, estate, planstate, totaltime) are deliberately left null until ExecutorStart is called.

## Parameters / Member Variables
- : The planned statement tree containing the execution plan
- : The original SQL query text for debugging and logging purposes  
- : The snapshot to use for reading data during query execution
- : Additional snapshot for referential integrity checks
- : The destination receiver that will handle query output
- : Parameter values to be substituted into the query
- : Query environment containing additional execution context
- : Flags controlling query instrumentation and timing

## Dependencies
- Functions called/Symbols referenced:
  - [PlannedStmt](../P/PlannedStmt.md)
  - DestReceiver  
  - [ParamListInfo](../P/ParamListInfo.md)
  - QueryEnvironment
  - QueryDesc
  - RegisterSnapshot (called twice)
- Called from (representative examples):
  - [ProcessQuery](../P/ProcessQuery.md)
  - [PortalStart](../P/PortalStart.md)
  - [_SPI_execute_plan](../S/_SPI_execute_plan.md)
  - [ExplainOnePlan](../E/ExplainOnePlan.md)
  - [postquel_start](../p/postquel_start.md)

## Notes and Other Information
The function performs snapshot registration to ensure proper memory management and transaction isolation. The QueryDesc structure returned by this function must later be freed using FreeQueryDesc to prevent memory leaks. The already_executed flag is initialized to false and will be set during query execution to prevent double execution.
# ExecutorRun

## Location
src/backend/executor/execMain.c: 299 - 309

## Overview
ExecutorRun is the main routine of the executor module that accepts a query descriptor and executes the query plan, providing a hook mechanism for plugins while delegating to the standard implementation.

## Definition
```c
void ExecutorRun(QueryDesc *queryDesc, ScanDirection direction, uint64 count, bool execute_once)
```

## Detailed Description
ExecutorRun serves as the primary interface for executing query plans in PostgreSQL. This function is the main entry point that coordinates the actual execution of queries after initialization by ExecutorStart. It supports flexible execution control through direction and count parameters, allowing for both forward and backward scanning as well as limited tuple retrieval.

The function provides a plugin architecture similar to ExecutorStart through the ExecutorRun_hook variable, enabling loadable plugins to intercept and customize the query execution process. When no hook is installed, it delegates to standard_ExecutorRun for the default execution behavior.

ExecutorRun handles various execution scenarios, from simple tuple retrieval to complex operations involving modifications. The function can run to completion (when count = 0) or retrieve a specific number of tuples. Output tuples are sent to the destination receiver specified in the QueryDesc, and execution statistics are maintained in the estate structure.

## Parameters / Member Variables
- `queryDesc`: A QueryDesc structure containing the initialized query execution context (ExecutorStart must have been called already)
- `direction`: ScanDirection specifying the direction of tuple retrieval; NoMovementScanDirection does nothing except start up/shut down the destination
- `count`: Maximum number of tuples to retrieve; 0 means no limit (run to completion); applies only to retrieved tuples, not to inserted/updated/deleted tuples
- `execute_once`: Legacy parameter present for API compatibility, currently ignored in the implementation

## Dependencies
- Functions called/Symbols referenced:
  - [standard_ExecutorRun](../s/standard_ExecutorRun.md) (default implementation when no hook is present)
  - QueryDesc (parameter structure)
  - ScanDirection (enumerated type for scan direction)
- Called from (representative examples):
  - [DoCopyTo](../D/DoCopyTo.md) (src/backend/commands/copyto.c:883)
  - [ExecCreateTableAs](ExecCreateTableAs.md) (src/backend/commands/createas.c:324)
  - [ExplainOnePlan](ExplainOnePlan.md) (src/backend/commands/explain.c:702)
  - [ProcessQuery](../P/ProcessQuery.md) (src/backend/tcop/pquery.c:160)
  - [PortalRunSelect](../P/PortalRunSelect.md) (src/backend/tcop/pquery.c:922)
  - [_SPI_pquery](../S/_SPI_pquery.md) (src/backend/executor/spi.c:2932)

## Notes and Other Information
- Must be called after ExecutorStart has properly initialized the query execution context
- The count limit applies only to retrieved tuples, not to rows affected by INSERT/UPDATE/DELETE operations
- Execution statistics are available in estate->es_processed (current call) and estate->es_total_processed (cumulative)
- No return value; results are communicated through the destination receiver and execution state
- The hook mechanism allows extensions to completely replace or wrap the standard executor execution behavior
- The execute_once parameter exists for API stability but is not actively used in current implementation
- Located at src/backend/executor/execMain.c:299-309
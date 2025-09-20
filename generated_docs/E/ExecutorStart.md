# ExecutorStart

## Location
[src/backend/executor/execMain.c:121-139](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L121-L139)

## Overview
ExecutorStart is the entry point function that must be called at the beginning of any execution of any query plan, providing a hook mechanism for plugins while delegating to the standard implementation.

## Definition

```c
void
ExecutorStart(QueryDesc *queryDesc, int eflags)
```
## Detailed Description
ExecutorStart serves as the primary interface for initiating query execution in PostgreSQL. It performs essential setup tasks including query ID reporting for statistics and provides an extensibility mechanism through function hooks. The function takes a QueryDesc that was previously created by CreateQueryDesc and fills in the tupDesc field to describe the tuples that will be returned, while also setting up internal fields (estate and planstate).

The function supports a plugin architecture through the ExecutorStart_hook variable, allowing loadable plugins to intercept and customize the executor startup process. When no hook is installed, it delegates to standard_ExecutorStart for the default behavior.

An important memory management aspect is that the CurrentMemoryContext when this function is called becomes the parent of the per-query context used for the entire Executor invocation.

## Parameters / Member Variables
- : A QueryDesc structure containing the parsed and planned query information, including the planned statement and other execution metadata
- : Flag bits as described in executor.h that control various aspects of execution behavior

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_report_query_id](../p/pgstat_report_query_id.md) (for query statistics reporting)
  - [standard_ExecutorStart](../s/standard_ExecutorStart.md) (default implementation when no hook is present)
  - QueryDesc (parameter structure)
- Called from (representative examples):
  - [BeginCopyTo](../B/BeginCopyTo.md) (src/backend/commands/copyto.c:569)
  - [ExecCreateTableAs](ExecCreateTableAs.md) (src/backend/commands/createas.c:321)
  - [ExplainOnePlan](ExplainOnePlan.md) (src/backend/commands/explain.c:688)
  - [ProcessQuery](../P/ProcessQuery.md) (src/backend/tcop/pquery.c:155)
  - [PortalStart](../P/PortalStart.md) (src/backend/tcop/pquery.c:517)
  - [_SPI_pquery](../S/_SPI_pquery.md) (src/backend/executor/spi.c:2930)

## Notes and Other Information
- The function ensures query_id reporting for cases where it might not have been reported earlier (e.g., EXECUTE statements or extended query protocol)
- Multiple calls to report the same query_id are harmless as duplicates are ignored
- The hook mechanism allows extensions to completely replace or wrap the standard executor startup behavior
- Located at src/backend/executor/execMain.c:121-139
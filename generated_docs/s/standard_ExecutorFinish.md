# standard_ExecutorFinish

## Location
[src/backend/executor/execMain.c:409-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L409-L459)

## Overview
Completes the execution phase of a query plan by performing final cleanup operations including ModifyTable node completion, trigger execution, and state finalization.

## Definition

```c
void
standard_ExecutorFinish(QueryDesc *queryDesc)
```
## Detailed Description
The  function is responsible for the final phase of query execution, handling post-processing operations that must occur after the main execution is complete. This function ensures that all ModifyTable nodes (INSERT, UPDATE, DELETE operations) are properly completed and executes any queued AFTER triggers. It also manages instrumentation timing and marks the executor state as finished to prevent duplicate execution.

The function operates within the query's memory context and includes comprehensive sanity checks to ensure it's called exactly once per executor instance. It's a critical component of the executor's lifecycle, bridging the gap between plan execution and final cleanup.

## Parameters / Member Variables
- : Pointer to the QueryDesc structure containing the query execution context, estate, and associated metadata

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - InstrStartNode
  - [ExecPostprocessPlan](../E/ExecPostprocessPlan.md)
  - [AfterTriggerEndQuery](../A/AfterTriggerEndQuery.md)
  - InstrStopNode
- Called from (representative examples):
  - [ExecutorFinish](../E/ExecutorFinish.md)

## Notes and Other Information
- Must be called exactly once per Executor instance (enforced by es_finished flag)
- Skips trigger execution if EXEC_FLAG_SKIP_TRIGGERS is set in estate flags
- Cannot be used with EXPLAIN ONLY queries (assertion check)
- Manages timing instrumentation for performance monitoring
- Switches to per-query memory context for proper memory management
- Sets es_finished flag to true to prevent re-execution
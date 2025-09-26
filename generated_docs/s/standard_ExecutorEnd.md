# standard_ExecutorEnd

## Location
[src/backend/executor/execMain.c:469-525](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/execMain.c#L469-L525)

## Overview
Performs comprehensive cleanup and resource deallocation at the end of query execution, including plan termination, snapshot cleanup, and memory context destruction.

## Definition

```c
void
standard_ExecutorEnd(QueryDesc *queryDesc)
```
## Detailed Description
The  function is responsible for the complete teardown of the executor environment after query execution. It performs a systematic cleanup process that includes terminating the execution plan, unregistering snapshots, and releasing all executor-allocated memory through the EState structure. This function ensures that all resources are properly freed and that the QueryDesc is reset to a safe state.

The function enforces that ExecutorFinish was previously called (except for EXPLAIN-only queries) to maintain proper executor lifecycle ordering. It operates within the per-query memory context during plan termination and then destroys that context as part of the EState cleanup.

## Parameters / Member Variables
- : Pointer to the QueryDesc structure containing the execution state, plan state, and associated resources to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [ExecEndPlan](../E/ExecEndPlan.md)
  - [UnregisterSnapshot](../U/UnregisterSnapshot.md)
  - [FreeExecutorState](../F/FreeExecutorState.md)
- Called from (representative examples):
  - [ExecutorEnd](../E/ExecutorEnd.md)

## Notes and Other Information
- Asserts that ExecutorFinish was called unless in EXPLAIN-only mode
- Unregisters both the main snapshot (es_snapshot) and crosscheck snapshot (es_crosscheck_snapshot)
- Resets QueryDesc fields (tupDesc, estate, planstate, totaltime) to NULL after cleanup
- Must switch out of per-query memory context before destroying it
- Releases all executor-allocated memory through FreeExecutorState
- Critical for preventing memory leaks in long-running database sessions
- Introduced validation for ExecutorFinish call in PostgreSQL 9.1
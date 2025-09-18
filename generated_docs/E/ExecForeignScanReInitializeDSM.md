# ExecForeignScanReInitializeDSM

## Location
[src/backend/executor/nodeForeignscan.c:397-417](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L397-L417)

## Overview
ExecForeignScanReInitializeDSM resets the shared state in dynamic shared memory before beginning a fresh parallel foreign scan.

## Definition
void ExecForeignScanReInitializeDSM(ForeignScanState *node, ParallelContext *pcxt)

## Detailed Description
This function reinitializes the coordination data in dynamic shared memory for parallel foreign scan operations. It looks up the previously allocated coordination segment using the plan node ID, then calls the foreign data wrapper's ReInitializeDSMForeignScan routine to reset the shared state. This is typically called when restarting a parallel scan operation that needs to begin fresh while reusing the existing shared memory infrastructure.

## Parameters / Member Variables
- : Pointer to the ForeignScanState containing the execution state for the foreign scan operation
- : Pointer to the ParallelContext structure used for coordinating parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_lookup](../s/shm_toc_lookup.md)
  - [FdwRoutine](../F/FdwRoutine.md).ReInitializeDSMForeignScan (if available)
- Called from (representative examples):
  - [ExecParallelReInitializeDSM](ExecParallelReInitializeDSM.md)

## Notes and Other Information
- Only performs reinitialization if the foreign data wrapper provides a ReInitializeDSMForeignScan routine
- Uses shm_toc_lookup with the plan_node_id to find the existing coordination segment
- The coordination memory segment must have been previously allocated by ExecForeignScanInitializeDSM
- Part of PostgreSQL's parallel query execution framework for scan restart scenarios
- Located in src/backend/executor/nodeForeignscan.c:397-417
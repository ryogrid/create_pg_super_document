# ExecForeignScanInitializeDSM

## Location
[src/backend/executor/nodeForeignscan.c:375-396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/nodeForeignscan.c#L375-L396)

## Overview
ExecForeignScanInitializeDSM initializes the dynamic shared memory coordination information for parallel foreign scan operations.

## Definition
void ExecForeignScanInitializeDSM(ForeignScanState *node, ParallelContext *pcxt)

## Detailed Description
This function initializes the shared memory segment used for coordinating parallel foreign scan operations. It allocates a chunk of dynamic shared memory using the size previously estimated by ExecForeignScanEstimate, then calls the foreign data wrapper's InitializeDSMForeignScan routine to set up the coordination data. Finally, it registers the coordination segment in the shared memory table of contents using the plan node ID as a key.

## Parameters / Member Variables
- : Pointer to the ForeignScanState containing the execution state for the foreign scan operation
- : Pointer to the ParallelContext structure used for coordinating parallel execution

## Dependencies
- Functions called/Symbols referenced:
  - [shm_toc_allocate](../s/shm_toc_allocate.md)
  - [shm_toc_insert](../s/shm_toc_insert.md)
  - [FdwRoutine](../F/FdwRoutine.md).InitializeDSMForeignScan (if available)
- Called from (representative examples):
  - [ExecParallelInitializeDSM](ExecParallelInitializeDSM.md)

## Notes and Other Information
- Only performs initialization if the foreign data wrapper provides an InitializeDSMForeignScan routine
- Uses the pscan_len value set by ExecForeignScanEstimate to allocate the correct amount of shared memory
- The plan_node_id is used as a unique key to identify this foreign scan's coordination data in shared memory
- Part of PostgreSQL's parallel query execution framework
- Located in src/backend/executor/nodeForeignscan.c:375-396
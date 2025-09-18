# ExecForeignScanInitializeWorker

## Location
src/backend/executor/nodeForeignscan.c: 418 - 440

## Overview
ExecForeignScanInitializeWorker initializes a parallel worker process for foreign scan operations using coordination information from dynamic shared memory.

## Definition
void ExecForeignScanInitializeWorker(ForeignScanState *node, ParallelWorkerContext *pwcxt)

## Detailed Description
This function initializes a parallel worker process that will participate in a foreign scan operation. It looks up the coordination data previously set up by the leader process in dynamic shared memory using the plan node ID, then calls the foreign data wrapper's InitializeWorkerForeignScan routine to perform worker-specific initialization. This allows the worker process to access shared state and coordinate its scanning activities with other workers.

## Parameters / Member Variables
- : Pointer to the ForeignScanState containing the execution state for the foreign scan operation
- : Pointer to the ParallelWorkerContext providing access to shared memory coordination data

## Dependencies
- Functions called/Symbols referenced:
  - shm_toc_lookup
  - FdwRoutine.InitializeWorkerForeignScan (if available)
- Called from (representative examples):
  - ExecParallelInitializeWorker

## Notes and Other Information
- Only performs initialization if the foreign data wrapper provides an InitializeWorkerForeignScan routine
- Uses shm_toc_lookup with the plan_node_id to find the coordination segment created by the leader
- The coordination memory must have been previously set up by ExecForeignScanInitializeDSM in the leader process
- Part of PostgreSQL's parallel query execution framework for worker process setup
- Located in src/backend/executor/nodeForeignscan.c:418-440
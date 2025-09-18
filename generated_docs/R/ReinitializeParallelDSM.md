# ReinitializeParallelDSM

## Location
src/backend/access/transam/parallel.c: 504 - 553

## Overview
Reinitializes the dynamic shared memory segment for a parallel context to prepare it for launching a new set of parallel workers after previous workers have completed.

## Definition


## Detailed Description
ReinitializeParallelDSM prepares an existing parallel context for reuse by cleaning up state from previous parallel worker executions and resetting the shared memory segment to a fresh state. This function is essential for scenarios where the same parallel context needs to be used multiple times, such as parallel vacuum operations that process multiple indexes or parallel query execution phases that require worker recycling.

The function first ensures all previous workers have completed and exited, then resets key state elements including the last WAL end position and recreates error communication queues. This allows the parallel context to be used again with LaunchParallelWorkers without requiring a complete recreation of the shared memory infrastructure.

## Parameters / Member Variables
- : The parallel context to reinitialize, containing the DSM segment and worker information that will be reset

## Dependencies
- Functions called/Symbols referenced:
  - WaitForParallelWorkersToFinish (ensures all workers complete their tasks)
  - WaitForParallelWorkersToExit (waits for worker processes to terminate)
  - [shm_toc_lookup](../s/shm_toc_lookup.md) (locates shared memory segments by key)
  - [shm_mq_create](../s/shm_mq_create.md), shm_mq_set_receiver, shm_mq_attach (recreates error message queues)
  - [FixedParallelState](../F/FixedParallelState.md) (structure containing parallel execution state)
  - PARALLEL_KEY_FIXED, PARALLEL_KEY_ERROR_QUEUE (shared memory keys)

- Called from (representative examples):
  - [parallel_vacuum_process_all_indexes](../p/parallel_vacuum_process_all_indexes.md) (parallel vacuum reinitialization)
  - [ExecParallelReinitialize](../E/ExecParallelReinitialize.md) (parallel query execution reinitialization)

## Notes and Other Information
- Must wait for all previous workers to finish and exit before reinitializing
- Cleans up known_attached_workers array and resets worker count to zero
- Resets the last_xlog_end position in FixedParallelState to allow fresh WAL tracking
- Recreates error message queues to ensure clean communication channels for new workers
- More efficient than destroying and recreating the entire parallel context
- Preserves the underlying DSM segment and most serialized state, only resetting runtime elements
- Essential for multi-phase parallel operations that reuse the same worker pool
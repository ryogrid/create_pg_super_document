# ExecParallelReinitialize

## Location
src/backend/executor/execParallel.c: 904 - 952

## Overview
ExecParallelReinitialize resets the parallel execution environment to prepare for launching a fresh batch of workers while reusing existing shared memory infrastructure and updating runtime parameters.

## Definition


## Detailed Description
This function enables reuse of parallel execution infrastructure by reinitializing shared memory state without complete teardown and reconstruction. It is particularly useful in scenarios where parallel execution needs to be restarted with updated parameters or when workers need to be relaunched after completion.

The reinitialization process involves several coordinated steps:

1. **Validation**: Ensures that previous workers have been properly shut down before proceeding
2. **Parameter Re-evaluation**: Forces evaluation of any initplan parameters that need to be passed to the new worker batch
3. **DSM Reinitialization**: Calls ReinitializeParallelDSM to reset shared memory structures while preserving the memory layout
4. **Communication Reset**: Re-establishes tuple queues using existing shared memory space rather than allocating new space
5. **Parameter Management**: Frees previously serialized parameters and serializes current parameter values if needed
6. **Plan Node Reset**: Traverses the plan tree to allow each node to reset its DSM-related state

Key optimizations include:
- Reusing existing shared memory allocation rather than complete recreation
- Preserving the overall memory layout and structure sizes
- Updating only the dynamic state that changes between worker batches
- Maintaining instrumentation and other persistent structures

This approach is more efficient than complete teardown and recreation, especially for queries that require multiple rounds of parallel execution.

## Parameters / Member Variables
- : Root plan state node for the parallel execution tree
- : ParallelExecutorInfo containing shared memory state and worker infrastructure
- : Bitmapset specifying which execution parameters to pass to new workers

## Dependencies
- Functions called/Symbols referenced:
  - ExecSetParamPlanMulti (re-evaluate initplan parameters)
  - GetPerTupleExprContext (get expression evaluation context)
  - ReinitializeParallelDSM (reset parallel DSM infrastructure)
  - ExecParallelSetupTupleQueues (re-establish tuple communication, reinitialize=true)
  - shm_toc_lookup (find fixed executor state in shared memory)
  - dsa_free, SerializeParamExecParams (manage parameter serialization)
  - ExecParallelReInitializeDSM (reset plan node DSM state)
  - bms_is_empty, DsaPointerIsValid (utility functions)
- Called from:
  - ExecGather (when restarting Gather node execution)
  - ExecGatherMerge (when restarting GatherMerge node execution)

## Notes and Other Information
- Requires that previous workers be completely finished before reinitialization (checked via assertion)
- The function preserves the shared memory segment and overall structure while resetting dynamic state
- Parameter serialization is handled carefully - old parameters are freed before new ones are serialized
- Tuple queue setup uses reinitialize=true mode to reuse existing shared memory space
- The DSA area is temporarily installed in the estate during plan node reinitialization
- Reader array is reset to NULL, requiring subsequent call to ExecParallelCreateReaders
- This function enables efficient restart of parallel execution without full infrastructure teardown
- All plan nodes get a chance to reset their parallel state through ExecParallelReInitializeDSM traversal
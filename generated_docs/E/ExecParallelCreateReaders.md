# ExecParallelCreateReaders

## Location
src/backend/executor/execParallel.c: 878 - 903

## Overview
ExecParallelCreateReaders creates TupleQueueReader objects for each launched parallel worker process, enabling the main backend to read result tuples from the shared memory queues established during parallel plan initialization.

## Definition


## Detailed Description
This function completes the setup of the tuple communication infrastructure for parallel query execution by creating reader objects that allow the coordinator process to consume results from worker processes. It is designed to be called after workers have been launched and are potentially already executing, making it separate from the initial plan setup.

The function operates by:
1. **Reader Array Allocation**: Allocates an array to hold TupleQueueReader pointers, sized according to the actual number of launched workers
2. **Queue Handle Association**: Associates each tuple queue with its corresponding worker's background worker handle for proper communication setup
3. **Reader Creation**: Creates a TupleQueueReader for each worker's tuple queue, establishing the reading endpoint for result collection

This separation from ExecInitParallelPlan allows for flexibility in the timing of worker launch versus reader setup. Workers can begin executing their portion of the query while the coordinator prepares the infrastructure to collect their results.

The function ensures that the coordinator has the necessary objects to read tuples from all active workers in a coordinated manner, supporting both ordered (GatherMerge) and unordered (Gather) result collection patterns.

## Parameters / Member Variables
- : ParallelExecutorInfo structure containing:
  - : Parallel context with worker information and launched worker count
  - : Array of shared memory queue handles for communication
  - : Array of TupleQueueReader pointers (initially NULL, populated by this function)

## Dependencies
- Functions called/Symbols referenced:
  - palloc (memory allocation for reader array)
  - shm_mq_set_handle (associate queue with worker background handle)
  - CreateTupleQueueReader (create reader object for tuple queue)
  - TupleQueueReader (reader object type)
- Called from:
  - ExecGather (Gather node execution setup)
  - ExecGatherMerge (GatherMerge node execution setup)

## Notes and Other Information
- This function is separate from ExecInitParallelPlan to allow workers to start before readers are created
- The function only creates readers if workers were actually launched (nworkers > 0)
- Uses the actual number of launched workers rather than the originally requested number
- Each reader corresponds one-to-one with a launched worker process
- The function includes an assertion to ensure readers haven't been created previously
- Background worker handles are used to properly associate queues with their respective worker processes
- After this function completes, the coordinator can begin reading tuples from workers using the created TupleQueueReader objects
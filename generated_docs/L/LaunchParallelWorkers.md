# LaunchParallelWorkers

## Location
src/backend/access/transam/parallel.c: 569 - 688

## Overview
Launches the actual background worker processes for a parallel context, registering them with PostgreSQL's background worker infrastructure and establishing communication channels.

## Definition


## Detailed Description
LaunchParallelWorkers is responsible for the actual creation and registration of background worker processes that will execute parallel work. This function configures BackgroundWorker structures with appropriate parameters, registers them with PostgreSQL's dynamic background worker system, and establishes the necessary communication infrastructure including message queue handles for error reporting.

The function handles registration failures gracefully by continuing to launch as many workers as possible rather than failing entirely. This resilient approach is essential because hitting system limits (like max_worker_processes) should degrade performance rather than cause query failures. The function also establishes the leader process as a lock group leader to coordinate resource access among all parallel workers.

Each worker is configured to execute the ParallelWorkerMain function with the DSM segment handle as its argument, allowing workers to attach to the shared memory and access all the serialized state information prepared by InitializeParallelDSM.

## Parameters / Member Variables
- : The parallel context containing the DSM segment and worker configuration that will be used to launch the background processes

## Dependencies
- Functions called/Symbols referenced:
  - BecomeLockGroupLeader (establishes lock group coordination)
  - RegisterDynamicBackgroundWorker (registers worker with background worker infrastructure)
  - dsm_segment_handle (obtains handle to shared memory segment)
  - shm_mq_set_handle, shm_mq_detach (manages message queue handles)
  - BackgroundWorker (structure type for worker configuration)
  - UInt32GetDatum (converts segment handle to Datum for worker argument)

- Called from (representative examples):
  - _brin_begin_parallel (launches workers for BRIN index operations)
  - _bt_begin_parallel (launches workers for B-tree index operations)
  - parallel_vacuum_process_all_indexes (launches workers for vacuum operations)
  - ExecGather, ExecGatherMerge (launches workers for parallel query execution)

## Notes and Other Information
- Gracefully handles worker registration failures by continuing with fewer workers
- Establishes the launching process as a lock group leader for coordination
- Each worker receives a unique index in bgw_extra to identify itself
- Workers are configured to start at BgWorkerStart_ConsistentState to ensure database consistency
- Failed worker registrations trigger cleanup of allocated message queues to prevent resource leaks
- The function tolerates ending up with fewer workers than requested due to system constraints
- Initializes the known_attached_workers tracking array based on actual launched worker count
- Uses "ParallelWorkerMain" as the entry point function for all parallel workers
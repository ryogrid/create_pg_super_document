# LaunchParallelWorkers

## Location
[src/backend/access/transam/parallel.c:569-688](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L569-L688)

## Overview
Launches the actual background worker processes for a parallel context, registering them with PostgreSQL's background worker infrastructure and establishing communication channels.

## Definition

```c
void
LaunchParallelWorkers(ParallelContext *pcxt)
```
## Detailed Description
LaunchParallelWorkers is responsible for the actual creation and registration of background worker processes that will execute parallel work. This function configures BackgroundWorker structures with appropriate parameters, registers them with PostgreSQL's dynamic background worker system, and establishes the necessary communication infrastructure including message queue handles for error reporting.

The function handles registration failures gracefully by continuing to launch as many workers as possible rather than failing entirely. This resilient approach is essential because hitting system limits (like max_worker_processes) should degrade performance rather than cause query failures. The function also establishes the leader process as a lock group leader to coordinate resource access among all parallel workers.

Each worker is configured to execute the ParallelWorkerMain function with the DSM segment handle as its argument, allowing workers to attach to the shared memory and access all the serialized state information prepared by InitializeParallelDSM.

## Parameters / Member Variables
- : The parallel context containing the DSM segment and worker configuration that will be used to launch the background processes

## Dependencies
- Functions called/Symbols referenced:
  - [BecomeLockGroupLeader](../B/BecomeLockGroupLeader.md) (establishes lock group coordination)
  - [RegisterDynamicBackgroundWorker](../R/RegisterDynamicBackgroundWorker.md) (registers worker with background worker infrastructure)
  - [dsm_segment_handle](../d/dsm_segment_handle.md) (obtains handle to shared memory segment)
  - [shm_mq_set_handle](../s/shm_mq_set_handle.md), shm_mq_detach (manages message queue handles)
  - [BackgroundWorker](../B/BackgroundWorker.md) (structure type for worker configuration)
  - [UInt32GetDatum](../U/UInt32GetDatum.md) (converts segment handle to Datum for worker argument)

- Called from (representative examples):
  - [_brin_begin_parallel](../b/_brin_begin_parallel.md) (launches workers for BRIN index operations)
  - [_bt_begin_parallel](../b/_bt_begin_parallel.md) (launches workers for B-tree index operations)
  - [parallel_vacuum_process_all_indexes](../p/parallel_vacuum_process_all_indexes.md) (launches workers for vacuum operations)
  - [ExecGather](../E/ExecGather.md), ExecGatherMerge (launches workers for parallel query execution)

## Notes and Other Information
- Gracefully handles worker registration failures by continuing with fewer workers
- Establishes the launching process as a lock group leader for coordination
- Each worker receives a unique index in bgw_extra to identify itself
- Workers are configured to start at BgWorkerStart_ConsistentState to ensure database consistency
- Failed worker registrations trigger cleanup of allocated message queues to prevent resource leaks
- The function tolerates ending up with fewer workers than requested due to system constraints
- Initializes the known_attached_workers tracking array based on actual launched worker count
- Uses "ParallelWorkerMain" as the entry point function for all parallel workers

## Simplified Source

```c
void LaunchParallelWorkers(ParallelContext *pcxt)
{
    MemoryContext oldcontext;
    BackgroundWorker worker;
    bool any_registrations_failed = false;

    // Skip if no workers to launch
    if (pcxt->nworkers == 0 || pcxt->nworkers_to_launch == 0)
        return;

    // Establish this process as the lock group leader
    BecomeLockGroupLeader();

    // Must have a DSM segment for workers to attach to
    Assert(pcxt->seg != NULL);

    // Switch to transaction context for persistent allocations
    oldcontext = MemoryContextSwitchTo(TopTransactionContext);

    // Configure background worker template
    memset(&worker, 0, sizeof(worker));
    snprintf(worker.bgw_name, BGW_MAXLEN, "parallel worker for PID %d", MyProcPid);
    snprintf(worker.bgw_type, BGW_MAXLEN, "parallel worker");
    worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION | BGWORKER_CLASS_PARALLEL;
    worker.bgw_start_time = BgWorkerStart_ConsistentState;
    worker.bgw_restart_time = BGW_NEVER_RESTART;
    sprintf(worker.bgw_library_name, "postgres");
    sprintf(worker.bgw_function_name, "ParallelWorkerMain");
    worker.bgw_main_arg = UInt32GetDatum(dsm_segment_handle(pcxt->seg));  // Pass DSM handle
    worker.bgw_notify_pid = MyProcPid;

    // Launch each worker process
    for (int i = 0; i < pcxt->nworkers_to_launch; ++i)
    {
        // Give each worker a unique index identifier
        memcpy(worker.bgw_extra, &i, sizeof(int));

        // Try to register this worker
        if (!any_registrations_failed &&
            RegisterDynamicBackgroundWorker(&worker, &pcxt->worker[i].bgwhandle))
        {
            // Success: set up error message queue handle
            shm_mq_set_handle(pcxt->worker[i].error_mqh, pcxt->worker[i].bgwhandle);
            pcxt->nworkers_launched++;
        }
        else
        {
            // Registration failed (likely hit max_worker_processes limit)
            any_registrations_failed = true;
            pcxt->worker[i].bgwhandle = NULL;

            // Clean up error queue to avoid waiting for worker that won't start
            shm_mq_detach(pcxt->worker[i].error_mqh);
            pcxt->worker[i].error_mqh = NULL;
        }
    }

    // Initialize tracking array for attached workers
    if (pcxt->nworkers_launched > 0)
    {
        pcxt->known_attached_workers = palloc0(sizeof(bool) * pcxt->nworkers_launched);
        pcxt->nknown_attached_workers = 0;
    }

    // Restore original memory context
    MemoryContextSwitchTo(oldcontext);
}
```
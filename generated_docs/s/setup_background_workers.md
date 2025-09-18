# setup_background_workers

## Location
src/test/modules/test_shm_mq/setup.c: 175 - 245

## Overview
This static function registers and creates background worker processes for shared memory message queue testing, handling proper memory management and cleanup registration.

## Definition
```c
static worker_state *setup_background_workers(int nworkers, dsm_segment *seg)
```

## Detailed Description
The `setup_background_workers` function creates and registers the specified number of background worker processes for shared memory message queue testing. It allocates a `worker_state` structure to track all worker handles, configures each worker with appropriate parameters, and registers them with the PostgreSQL background worker subsystem. The function ensures proper memory context management by allocating worker state in `TopTransactionContext` to persist beyond expression evaluation contexts.

A critical aspect of this function is its error handling and cleanup strategy. It registers a cleanup callback with the dynamic shared memory segment to handle cases where workers fail to start or exit unexpectedly before completing initialization. The function also provides detailed error reporting when worker registration fails, suggesting configuration adjustments to resolve resource limitations.

Each worker is configured to execute the `test_shm_mq_main` function from the `test_shm_mq` library, with access to shared memory and notification capabilities enabled. The workers are designed to run only when the database reaches a consistent state and never restart automatically.

## Parameters / Member Variables
- `nworkers`: Number of background worker processes to create and register
- `seg`: Pointer to the dynamic shared memory segment that workers will use for communication

## Dependencies
- Functions called/Symbols referenced:
  - MemoryContextSwitchTo
  - MemoryContextAlloc
  - on_dsm_detach
  - cleanup_background_workers
  - RegisterDynamicBackgroundWorker
  - dsm_segment_handle
  - UInt32GetDatum
  - sprintf/snprintf
- Called from (representative examples):
  - test_shm_mq_setup

## Notes and Other Information
- This is a static function internal to the test_shm_mq module, located in `src/test/modules/test_shm_mq/setup.c:175-245`
- Uses `TopTransactionContext` for memory allocation to ensure worker state persists for the duration of the test
- Registers cleanup callback `cleanup_background_workers` to handle premature worker termination scenarios
- Each worker is configured with `BGWORKER_SHMEM_ACCESS` flag to enable shared memory access
- Workers start at `BgWorkerStart_ConsistentState` and have `BGW_NEVER_RESTART` restart policy
- The dynamic shared memory segment handle is passed as the main argument to each worker process
- Worker notification is enabled by setting `bgw_notify_pid` to the current process ID
- Returns a `worker_state` structure containing handles to all successfully registered workers
- Provides helpful error messages suggesting to increase `max_worker_processes` when registration fails
- The function maintains proper error state by incrementing `wstate->nworkers` only after successful registration
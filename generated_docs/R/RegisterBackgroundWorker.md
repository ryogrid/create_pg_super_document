# RegisterBackgroundWorker

## Location
[src/backend/postmaster/bgworker.c:862-969](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L862-L969)

## Overview
Registers a new static background worker that will be started by the postmaster during server startup or when libraries are loaded.

## Definition

```c
structure.
	 */
	if (!IsUnderPostmaster)
		return false;
```
## Detailed Description
This function registers a static background worker with the PostgreSQL system. Static workers are registered during server startup, either directly in the postmaster process or via the _PG_init function of modules loaded through shared_preload_libraries. The function performs several validation checks including process context verification, worker configuration validation, and resource limit enforcement before adding the worker to the global background worker list.

The function ensures that workers are only registered at appropriate times (before shared memory initialization) and in the correct process context (postmaster only). It maintains a count of registered workers to enforce the max_worker_processes limit and stores the worker configuration in the PostmasterContext for later use during worker startup.

## Parameters / Member Variables
- : Pointer to a BackgroundWorker structure containing the worker configuration including name, library, function, flags, and other parameters

## Dependencies
- Functions called/Symbols referenced:
  - ereport, elog, errmsg, errcode, errmsg_internal, errdetail_plural, errhint (error reporting)
  - [SanityCheckBackgroundWorker](../S/SanityCheckBackgroundWorker.md) (worker validation)
  - [MemoryContextAllocExtended](../M/MemoryContextAllocExtended.md) (memory allocation)
  - [slist_push_head](../s/slist_push_head.md) (list management)
- Constants referenced:
  - LOG, DEBUG1, ERROR (error levels)
  - ERRCODE_FEATURE_NOT_SUPPORTED, ERRCODE_CONFIGURATION_LIMIT_EXCEEDED, ERRCODE_OUT_OF_MEMORY
  - MCXT_ALLOC_NO_OOM (memory allocation flag)
- Global variables accessed:
  - IsUnderPostmaster, IsPostmasterEnvironment (process context)
  - process_shared_preload_libraries_in_progress (loading state)
  - BackgroundWorkerData (shared memory state)
  - max_worker_processes (configuration limit)
  - PostmasterContext (memory context)
  - BackgroundWorkerList (worker list)
- Data structures used:
  - [RegisteredBgWorker](RegisteredBgWorker.md) (worker registration entry)
- Called from:
  - [ApplyLauncherRegister](../A/ApplyLauncherRegister.md) (logical replication)
  - [_PG_init](../P/_PG_init.md) functions in extension modules

## Notes and Other Information
- This function is for static workers only - dynamic workers use RegisterDynamicBackgroundWorker
- Can only be called from the postmaster process or _PG_init functions during shared_preload_libraries loading
- Static workers cannot request notification (bgw_notify_pid must be 0)
- Workers are limited by the max_worker_processes configuration parameter
- Registration must occur before BackgroundWorkerShmemInit() is called
- The function uses LOG level for most errors to avoid stopping server startup
- Memory allocation failures are handled gracefully with error reporting
- Workers are stored in a singly-linked list for efficient management

## Simplified Source

```c
// Simplified version of RegisterBackgroundWorker
void RegisterBackgroundWorker(BackgroundWorker *worker) {
    static int numworkers = 0;

    // Core logic step 1: Verify we're in the postmaster process
    if (IsUnderPostmaster || !IsPostmasterEnvironment) {
        if (process_shared_preload_libraries_in_progress)
            return;  // Allow but ignore during library loading
        ereport(LOG, (errmsg("background worker must be registered in shared_preload_libraries")));
        return;
    }

    // Core logic step 2: Ensure registration happens before shared memory init
    if (BackgroundWorkerData != NULL) {
        elog(ERROR, "cannot register background worker after shmem init");
    }

    // Core logic step 3: Validate worker configuration
    if (!SanityCheckBackgroundWorker(worker, LOG))
        return;

    // Core logic step 4: Static workers cannot request notification
    if (worker->bgw_notify_pid != 0) {
        ereport(LOG, (errmsg("only dynamic background workers can request notification")));
        return;
    }

    // Core logic step 5: Enforce worker count limit
    if (++numworkers > max_worker_processes) {
        ereport(LOG, (errmsg("too many background workers")));
        return;
    }

    // Core logic step 6: Allocate and initialize worker registration entry
    RegisteredBgWorker *rw = MemoryContextAllocExtended(PostmasterContext,
                                                        sizeof(RegisteredBgWorker),
                                                        MCXT_ALLOC_NO_OOM);
    if (rw == NULL) {
        ereport(LOG, (errmsg("out of memory")));
        return;
    }

    // Core logic step 7: Initialize the registered worker structure
    rw->rw_worker = *worker;  // Copy worker configuration
    rw->rw_backend = NULL;
    rw->rw_pid = 0;
    rw->rw_child_slot = 0;
    rw->rw_crashed_at = 0;
    rw->rw_terminate = false;

    // Core logic step 8: Add to global worker list
    slist_push_head(&BackgroundWorkerList, &rw->rw_lnode);
}
```

Key simplifications made:
- Removed detailed error code specifications for clarity
- Consolidated verbose error messages into simpler versions
- Removed complex plural message handling
- Abstracted detailed memory context operations
- Focused on the main execution path and core validation steps
- Simplified comments to highlight the key logic flow
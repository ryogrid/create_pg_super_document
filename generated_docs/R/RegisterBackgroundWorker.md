# RegisterBackgroundWorker

## Location
src/backend/postmaster/bgworker.c: 862 - 969

## Overview
Registers a new static background worker that will be started by the postmaster during server startup or when libraries are loaded.

## Definition


## Detailed Description
This function registers a static background worker with the PostgreSQL system. Static workers are registered during server startup, either directly in the postmaster process or via the _PG_init function of modules loaded through shared_preload_libraries. The function performs several validation checks including process context verification, worker configuration validation, and resource limit enforcement before adding the worker to the global background worker list.

The function ensures that workers are only registered at appropriate times (before shared memory initialization) and in the correct process context (postmaster only). It maintains a count of registered workers to enforce the max_worker_processes limit and stores the worker configuration in the PostmasterContext for later use during worker startup.

## Parameters / Member Variables
- : Pointer to a BackgroundWorker structure containing the worker configuration including name, library, function, flags, and other parameters

## Dependencies
- Functions called/Symbols referenced:
  - ereport, elog, errmsg, errcode, errmsg_internal, errdetail_plural, errhint (error reporting)
  - SanityCheckBackgroundWorker (worker validation)
  - MemoryContextAllocExtended (memory allocation)
  - slist_push_head (list management)
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
  - RegisteredBgWorker (worker registration entry)
- Called from:
  - ApplyLauncherRegister (logical replication)
  - _PG_init functions in extension modules

## Notes and Other Information
- This function is for static workers only - dynamic workers use RegisterDynamicBackgroundWorker
- Can only be called from the postmaster process or _PG_init functions during shared_preload_libraries loading
- Static workers cannot request notification (bgw_notify_pid must be 0)
- Workers are limited by the max_worker_processes configuration parameter
- Registration must occur before BackgroundWorkerShmemInit() is called
- The function uses LOG level for most errors to avoid stopping server startup
- Memory allocation failures are handled gracefully with error reporting
- Workers are stored in a singly-linked list for efficient management
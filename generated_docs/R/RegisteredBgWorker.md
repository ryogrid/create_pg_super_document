# RegisteredBgWorker

## Location
src/include/postmaster/bgworker_internals.h: 33 - 43

## Overview
RegisteredBgWorker is an internal structure used by the PostgreSQL postmaster to track and manage background worker processes throughout their lifecycle.

## Definition


## Detailed Description
RegisteredBgWorker serves as the postmaster's internal representation of a background worker process. It extends the basic BackgroundWorker structure with additional runtime state information needed for process management, monitoring, and recovery. This structure is private to the postmaster and maintains the complete lifecycle state of each background worker from registration through termination.

The structure is designed to track both shared memory-connected workers and those that require database connections. Workers requesting database connections during registration will have their rw_backend field set and will be present in the BackendList, enabling proper resource management and cleanup.

## Parameters / Member Variables
- : The original BackgroundWorker registry entry containing the worker's configuration and metadata
- : Pointer to the worker's BackendList entry if it requires a database connection; NULL for shared memory-only workers
- : Process ID of the running worker; 0 indicates the worker is not currently running
- : Slot index used for tracking child processes in the postmaster's process management arrays
- : Timestamp of the worker's last crash; 0 if the worker has never crashed, used for restart throttling
- : Slot index in shared memory structures for inter-process communication and coordination
- : Boolean flag indicating whether the worker should be terminated during shutdown or restart scenarios
- : Linked list node for maintaining the postmaster's list of registered background workers

## Dependencies
- Functions called/Symbols referenced:
  - BackgroundWorker
  - bkend
  - pid_t
  - slist_node
- Called from (representative examples):
  - BackgroundWorkerShmemInit
  - FindRegisteredWorkerBySlotNumber
  - BackgroundWorkerStateChange
  - ForgetBackgroundWorker
  - ReportBackgroundWorkerPID
  - ReportBackgroundWorkerExit
  - RegisterBackgroundWorker
  - CleanupBackgroundWorker
  - do_start_bgworker

## Notes and Other Information
- This structure is defined in bgworker_internals.h and is private to the postmaster implementation
- The distinction between shared memory-connected and database-connected workers is critical for proper resource management
- The rw_crashed_at timestamp is used to implement crash throttling and prevent rapid restart loops
- Workers are managed through a singly-linked list using the rw_lnode member
- The structure supports both one-time and continuously running background workers
- Proper cleanup of both the RegisteredBgWorker structure and associated backend resources is essential to prevent resource leaks
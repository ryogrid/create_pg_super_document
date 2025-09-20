# RegisteredBgWorker

## Location
[src/include/postmaster/bgworker_internals.h:33-43](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/postmaster/bgworker_internals.h#L33-L43)

## Overview
RegisteredBgWorker is an internal structure used by the PostgreSQL postmaster to track and manage background worker processes throughout their lifecycle.

## Definition

```c
typedef struct RegisteredBgWorker
{
	BackgroundWorker rw_worker; /* its registry entry */
	struct bkend *rw_backend;	/* its BackendList entry, or NULL */
	pid_t		rw_pid;			/* 0 if not running */
	int			rw_child_slot;
	TimestampTz rw_crashed_at;	/* if not 0, time it last crashed */
	int			rw_shmem_slot;
	bool		rw_terminate;
	slist_node	rw_lnode;		/* list link */
} RegisteredBgWorker;
```
## Detailed Description
RegisteredBgWorker serves as the postmaster's internal representation of a background worker process. It extends the basic BackgroundWorker structure with additional runtime state information needed for process management, monitoring, and recovery. This structure is private to the postmaster and maintains the complete lifecycle state of each background worker from registration through termination.

The structure is designed to track both shared memory-connected workers and those that require database connections. Workers requesting database connections during registration will have their rw_backend field set and will be present in the BackendList, enabling proper resource management and cleanup.

## Parameters / Member Variables
- `rw_worker`: The original BackgroundWorker registry entry containing the worker's configuration and metadata
- `*rw_backend`: Pointer to the worker's BackendList entry if it requires a database connection; NULL for shared memory-only workers
- `rw_pid`: Process ID of the running worker; 0 indicates the worker is not currently running
- `rw_child_slot`: Slot index used for tracking child processes in the postmaster's process management arrays
- `rw_crashed_at`: Timestamp of the worker's last crash; 0 if the worker has never crashed, used for restart throttling
- `rw_shmem_slot`: Slot index in shared memory structures for inter-process communication and coordination
- `rw_terminate`: Boolean flag indicating whether the worker should be terminated during shutdown or restart scenarios
- `rw_lnode`: Linked list node for maintaining the postmaster's list of registered background workers
## Dependencies
- Functions called/Symbols referenced:
  - [BackgroundWorker](../B/BackgroundWorker.md)
  - bkend
  - pid_t
  - [slist_node](../s/slist_node.md)
- Called from (representative examples):
  - [BackgroundWorkerShmemInit](../B/BackgroundWorkerShmemInit.md)
  - [FindRegisteredWorkerBySlotNumber](../F/FindRegisteredWorkerBySlotNumber.md)
  - [BackgroundWorkerStateChange](../B/BackgroundWorkerStateChange.md)
  - [ForgetBackgroundWorker](../F/ForgetBackgroundWorker.md)
  - [ReportBackgroundWorkerPID](ReportBackgroundWorkerPID.md)
  - [ReportBackgroundWorkerExit](ReportBackgroundWorkerExit.md)
  - [RegisterBackgroundWorker](RegisterBackgroundWorker.md)
  - [CleanupBackgroundWorker](../C/CleanupBackgroundWorker.md)
  - [do_start_bgworker](../d/do_start_bgworker.md)

## Notes and Other Information
- This structure is defined in bgworker_internals.h and is private to the postmaster implementation
- The distinction between shared memory-connected and database-connected workers is critical for proper resource management
- The rw_crashed_at timestamp is used to implement crash throttling and prevent rapid restart loops
- Workers are managed through a singly-linked list using the rw_lnode member
- The structure supports both one-time and continuously running background workers
- Proper cleanup of both the RegisteredBgWorker structure and associated backend resources is essential to prevent resource leaks
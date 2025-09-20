# LockInstanceData

## Location
[src/include/storage/lock.h:452-463](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L452-L463)

## Overview
LockInstanceData holds information about a specific lock instance for user-level lock listing functions, providing detailed lock status for monitoring and diagnostics.

## Definition

```c
typedef struct LockInstanceData
{
	LOCKTAG		locktag;		/* tag for locked object */
	LOCKMASK	holdMask;		/* locks held by this PGPROC */
	LOCKMODE	waitLockMode;	/* lock awaited by this PGPROC, if any */
	VirtualTransactionId vxid;	/* virtual transaction ID of this PGPROC */
	TimestampTz waitStart;		/* time at which this PGPROC started waiting
								 * for lock */
	int			pid;			/* pid of this PGPROC */
	int			leaderPid;		/* pid of group leader; = pid if no group */
	bool		fastpath;		/* taken via fastpath? */
} LockInstanceData;
```
## Detailed Description
LockInstanceData is a data transfer structure designed to communicate lock state information from PostgreSQL's internal lock manager to user-visible functions. This structure is primarily used by system functions like pg_locks, pg_blocking_pids, and related lock monitoring utilities that provide visibility into the database's locking state.

The structure captures a snapshot of a particular process's involvement with a specific lock, including what types of locks it holds, what it might be waiting for, timing information, and process identification details. This information is essential for lock monitoring, deadlock analysis, and performance troubleshooting.

## Parameters / Member Variables
- `locktag`: LOCKTAG identifying the specific object being locked (table, tuple, transaction, etc.)
- `holdMask`: Bitmask indicating which lock modes this process currently holds on the object
- `waitLockMode`: The lock mode this process is waiting to acquire, or invalid if not waiting
- `vxid`: Virtual transaction ID of the process holding or requesting the lock
- `waitStart`: Timestamp when the process started waiting for the lock (if applicable)
- `pid`: Process ID of the backend process involved with this lock
- `leaderPid`: Process ID of the parallel group leader, or same as pid if not in a parallel group
- `fastpath`: Boolean indicating whether this lock was acquired via the fast-path mechanism
## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG
  - LOCKMASK
  - LOCKMODE
  - [VirtualTransactionId](../V/VirtualTransactionId.md)
  - TimestampTz
- Called from (representative examples):
  - [GetLockStatusData](../G/GetLockStatusData.md)
  - [GetBlockerStatusData](../G/GetBlockerStatusData.md)
  - [GetSingleProcBlockerStatusData](../G/GetSingleProcBlockerStatusData.md)
  - [pg_lock_status](../p/pg_lock_status.md)
  - [pg_blocking_pids](../p/pg_blocking_pids.md)

## Notes and Other Information
LockInstanceData is specifically designed for external consumption rather than internal lock management. It provides a stable interface for lock introspection functions that expose lock information to database administrators and monitoring tools. The structure includes timing information that helps diagnose lock contention issues and the fastpath flag that indicates whether the lock bypassed the normal shared memory lock table for performance optimization.
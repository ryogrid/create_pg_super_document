# BlockedProcData

## Location
[src/include/storage/lock.h:471-482](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/lock.h#L471-L482)

## Overview
BlockedProcData is a structure that contains information about a blocked process, including its PID, associated locks, and position in wait queues within PostgreSQL's lock management system.

## Definition
```c
typedef struct BlockedProcData
{
    int         pid;            /* pid of a blocked PGPROC */
    /* Per-PROCLOCK information about PROCLOCKs of the lock the pid awaits */
    /* (these fields refer to indexes in BlockedProcsData.locks[]) */
    int         first_lock;     /* index of first relevant LockInstanceData */
    int         num_locks;      /* number of relevant LockInstanceDatas */
    /* PIDs of PGPROCs that are ahead of "pid" in the lock's wait queue */
    /* (these fields refer to indexes in BlockedProcsData.waiter_pids[]) */
    int         first_waiter;   /* index of first preceding waiter */
    int         num_waiters;    /* number of preceding waiters */
} BlockedProcData;
```

## Detailed Description
BlockedProcData represents detailed information about a single blocked process in PostgreSQL's lock system. It serves as a key component for deadlock detection, lock analysis, and debugging blocked queries. The structure efficiently organizes information about what locks a process is waiting for and which other processes are ahead of it in the wait queue. This design allows for comprehensive analysis of blocking relationships and wait dependencies in the system.

## Parameters / Member Variables
- `pid`: Process ID of the blocked PostgreSQL backend process
- `first_lock`: Starting index in the BlockedProcsData.locks[] array where this process's relevant lock information begins
- `num_locks`: Number of consecutive LockInstanceData entries in the locks array that pertain to this blocked process
- `first_waiter`: Starting index in the BlockedProcsData.waiter_pids[] array where the list of processes ahead of this one in the wait queue begins
- `num_waiters`: Number of processes that are positioned ahead of this process in the lock wait queue

## Dependencies
- Functions called/Symbols referenced:
  - (No direct references - this is a data structure)
- Called from (representative examples):
  - [GetBlockerStatusData](../G/GetBlockerStatusData.md)
  - [GetSingleProcBlockerStatusData](../G/GetSingleProcBlockerStatusData.md)
  - [pg_blocking_pids](../p/pg_blocking_pids.md)
  - [BlockedProcsData](BlockedProcsData.md)

## Notes and Other Information
- This structure is defined in src/include/storage/lock.h:471-482
- It works in conjunction with BlockedProcsData to provide a complete picture of blocking relationships
- The index-based approach (first_lock, num_locks, first_waiter, num_waiters) provides efficient access to relevant data while minimizing memory usage
- This structure is essential for PostgreSQL's deadlock detection algorithms and lock monitoring features
- Used extensively in system views and functions that report on blocked processes and lock contention
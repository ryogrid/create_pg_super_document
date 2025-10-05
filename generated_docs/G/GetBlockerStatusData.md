# GetBlockerStatusData

## Location
[src/backend/storage/lmgr/lock.c:3813-3892](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3813-L3892)

## Overview
GetBlockerStatusData returns a summary of the lock manager's state concerning locks that are blocking a specified PID or any member of the PID's lock group, providing detailed information about blocking relationships.

## Definition

```c
structure.  See notes
		 * in GetLockStatusData().
		 */
		for (i = 0;
```
## Detailed Description
This function analyzes the lock manager's state to identify and report on locks that are blocking a specific backend process or any member of its lock group. Unlike GetLockStatusData which provides a complete system-wide lock snapshot, this function focuses specifically on blocking relationships for a particular process.

The function operates by:
1. **Process identification**: Locating the specified backend PID in the process array
2. **Lock group analysis**: Determining if the process is part of a lock group and examining all group members
3. **Blocker collection**: For each waiting process, collecting information about the lock it's waiting for and the processes that hold conflicting locks
4. **Wait queue analysis**: Identifying which processes are ahead in the wait queue

The function handles both individual processes and lock groups. For lock groups (parallel query workers), it examines all group members to provide a complete picture of blocking relationships within the group.

## Parameters / Member Variables
- : The process ID of the backend whose blocking status should be analyzed
- : Array of BlockedProcData objects describing each blocked process
- : Array of LockInstanceData objects for locks involved in blocking
- : Array of PIDs representing processes ahead in wait queues
- , , : Counts of elements in respective arrays
- , , : Allocated sizes of respective arrays

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation
  - ,  - Lock management
  -  - Process lookup with locking
  -  - Individual process analysis
  - ,  - Lock group member iteration
  -  - Lock partition access

- Called from (representative examples):
  -  - SQL function for identifying blocking processes

## Notes and Other Information
- Requires holding both ProcArrayLock and all hash partition locks to ensure consistency
- Returns empty arrays if the specified PID is invalid or not waiting on any heavyweight locks
- Handles lock groups by examining all group members, providing comprehensive blocking analysis for parallel operations
- The function maintains the same locking discipline as GetLockStatusData, acquiring partition locks in order and releasing in reverse order
- Memory allocation uses pre-estimation based on MaxBackends to minimize reallocation while holding locks
- All returned lock tags for a single blocked PID should be the same, as a process can only wait on one lock at a time
- Wait queue information allows callers to determine the position of blocked processes relative to other waiters

## Simplified Source
```c
BlockedProcsData *GetBlockerStatusData(int blocked_pid)
{
    BlockedProcsData *data;
    PGPROC *proc;
    int i;

    data = (BlockedProcsData *) palloc(sizeof(BlockedProcsData));

    // Pre-allocate arrays based on MaxBackends estimate
    data->nprocs = data->nlocks = data->npids = 0;
    data->maxprocs = data->maxlocks = data->maxpids = MaxBackends;
    data->procs = (BlockedProcData *) palloc(sizeof(BlockedProcData) * data->maxprocs);
    data->locks = (LockInstanceData *) palloc(sizeof(LockInstanceData) * data->maxlocks);
    data->waiter_pids = (int *) palloc(sizeof(int) * data->maxpids);

    // Must hold ProcArrayLock to safely examine process entries
    LWLockAcquire(ProcArrayLock, LW_SHARED);

    proc = BackendPidGetProcWithLock(blocked_pid);

    if (proc != NULL) {
        // Acquire all partition locks for consistent lock table view
        for (i = 0; i < NUM_LOCK_PARTITIONS; i++)
            LWLockAcquire(LockHashPartitionLockByIndex(i), LW_SHARED);

        if (proc->lockGroupLeader == NULL) {
            // Simple case: process is not in a lock group
            GetSingleProcBlockerStatusData(proc, data);
        } else {
            // Complex case: examine all processes in the lock group
            dlist_iter iter;

            dlist_foreach(iter, &proc->lockGroupLeader->lockGroupMembers) {
                PGPROC *memberProc;

                memberProc = dlist_container(PGPROC, lockGroupLink, iter.cur);
                GetSingleProcBlockerStatusData(memberProc, data);
            }
        }

        // Release partition locks in reverse order
        for (i = NUM_LOCK_PARTITIONS; --i >= 0;)
            LWLockRelease(LockHashPartitionLockByIndex(i));
    }

    LWLockRelease(ProcArrayLock);
    return data;
}
```
# GetSingleProcBlockerStatusData

## Location
[src/backend/storage/lmgr/lock.c:3893-3987](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3893-L3987)

## Overview
GetSingleProcBlockerStatusData accumulates blocking data for a single process, collecting information about the lock it's waiting on and all processes that hold or wait for the same lock.

## Definition

```c
structure.
	 *
	 * Must grab LWLocks in partition-number order to avoid LWLock deadlock.
	 */
	for (i = 0;
```
## Detailed Description
This internal helper function analyzes a single process to determine its blocking status and contributes data to a BlockedProcsData structure. It's called by GetBlockerStatusData for each process that needs to be examined, whether individually or as part of a lock group.

The function operates in several phases:
1. **Blocking validation**: Checks if the process is actually waiting on a lock
2. **Process record creation**: Sets up a BlockedProcData entry for the process
3. **Lock holder collection**: Gathers all PROCLOCKs associated with the contested lock
4. **Wait queue analysis**: Collects PIDs of all processes ahead in the wait queue

The function only examines the main lock table (not fast-path arrays) since contested locks requiring wait queues cannot use the fast-path mechanism.

## Parameters / Member Variables
- : Pointer to the PGPROC structure of the potentially blocked process
- : Pointer to BlockedProcsData structure being populated with blocking information

The function modifies the  structure by:
- Adding a BlockedProcData entry to the  array
- Adding LockInstanceData entries to the  array for all lock holders
- Adding PIDs to the  array for processes ahead in the wait queue

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory reallocation for dynamic arrays
  - ,  - Iteration over lock and wait queue lists
  -  - Container extraction from list nodes
  -  - Count elements in wait queue
  -  - Memory copying for lock tags
  -  - Maximum value calculation

- Called from (representative examples):
  -  - For both individual processes and lock group members

## Notes and Other Information
- The function is static and only used internally within the lock manager
- Returns early if the process is not actually blocked (waitLock is NULL)
- Fast-path arrays are intentionally ignored since contested locks cannot use fast-path
- Memory arrays are dynamically expanded as needed during collection
- Wait queue traversal stops when it reaches the blocked process, ensuring only preceding waiters are collected
- The function maintains consistency between the three parallel arrays (procs, locks, waiter_pids) using index ranges
- Lock instance data includes both holding and waiting processes for the contested lock
- Process group leader PID is captured for each lock holder to support lock group analysis

## Simplified Source

```c
static void
GetSingleProcBlockerStatusData(PGPROC *blocked_proc, BlockedProcsData *data)
{
    LOCK *theLock = blocked_proc->waitLock;
    BlockedProcData *bproc;

    // Skip if process is not actually blocked
    if (theLock == NULL)
        return;

    // Set up process entry
    bproc = &data->procs[data->nprocs++];
    bproc->pid = blocked_proc->pid;
    bproc->first_lock = data->nlocks;
    bproc->first_waiter = data->npids;

    // Collect all lock holders for this contested lock
    dlist_foreach(proclock_iter, &theLock->procLocks)
    {
        PROCLOCK *proclock = dlist_container(PROCLOCK, lockLink, proclock_iter.cur);
        PGPROC *proc = proclock->tag.myProc;
        LOCK *lock = proclock->tag.myLock;

        // Expand locks array if needed
        if (data->nlocks >= data->maxlocks) {
            data->maxlocks += MaxBackends;
            data->locks = repalloc(data->locks, sizeof(LockInstanceData) * data->maxlocks);
        }

        // Record lock instance data
        LockInstanceData *instance = &data->locks[data->nlocks];
        memcpy(&instance->locktag, &lock->tag, sizeof(LOCKTAG));
        instance->holdMask = proclock->holdMask;
        instance->waitLockMode = (proc->waitLock == lock) ? proc->waitLockMode : NoLock;
        instance->vxid.procNumber = proc->vxid.procNumber;
        instance->vxid.localTransactionId = proc->vxid.lxid;
        instance->pid = proc->pid;
        instance->leaderPid = proclock->groupLeader->pid;
        instance->fastpath = false;
        data->nlocks++;
    }

    // Collect wait queue PIDs (processes ahead of blocked_proc)
    dclist_head *waitQueue = &(theLock->waitProcs);
    int queue_size = dclist_count(waitQueue);

    // Expand waiter PIDs array if needed
    if (queue_size > data->maxpids - data->npids) {
        data->maxpids = Max(data->maxpids + MaxBackends, data->npids + queue_size);
        data->waiter_pids = repalloc(data->waiter_pids, sizeof(int) * data->maxpids);
    }

    // Walk wait queue until we reach blocked_proc
    dclist_foreach(proc_iter, waitQueue)
    {
        PGPROC *queued_proc = dlist_container(PGPROC, links, proc_iter.cur);

        if (queued_proc == blocked_proc)
            break;
        data->waiter_pids[data->npids++] = queued_proc->pid;
    }

    // Record counts for this process
    bproc->num_locks = data->nlocks - bproc->first_lock;
    bproc->num_waiters = data->npids - bproc->first_waiter;
}
```
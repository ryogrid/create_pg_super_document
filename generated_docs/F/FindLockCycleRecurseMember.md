# FindLockCycleRecurseMember

## Location
[src/backend/storage/lmgr/deadlock.c:533-786](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/deadlock.c#L533-L786)

## Overview
FindLockCycleRecurseMember is a detailed helper function that examines the lock dependencies of a specific process member, checking for both hard-blocking and soft-blocking relationships in PostgreSQL's deadlock detection algorithm.

## Definition
static bool FindLockCycleRecurseMember(PGPROC *checkProc, PGPROC *checkProcLeader, int depth, EDGE *softEdges, int *nSoftEdges)

## Detailed Description
This function performs the detailed analysis of lock conflicts for a single process member within the deadlock detection algorithm. It examines two types of blocking relationships:

1. **Hard blocks**: Processes that already hold conflicting locks, creating immediate blocking relationships
2. **Soft blocks**: Processes ahead in the lock wait queue whose requests conflict with the current process

The function handles lock groups by considering the group leader when determining conflicts and ensures that processes within the same lock group don't block each other. It also includes special handling for autovacuum processes, allowing them to be canceled when they directly block user processes.

For relation extension locks, the function immediately returns false as these locks cannot participate in deadlock cycles by design.

## Parameters / Member Variables
- : The specific PGPROC being examined for lock conflicts
- : The leader of checkProc's lock group (may be checkProc itself)
- : Current depth in the deadlock detection recursion
- : Output array to collect soft edge information for potential deadlock resolution
- : Output parameter tracking the number of soft edges found

## Dependencies
- Functions called/Symbols referenced:
  - [GetLocksMethodTable](../G/GetLocksMethodTable.md)
  - dlist_foreach
  - dlist_container
  - dclist_foreach
  - [FindLockCycleRecurse](FindLockCycleRecurse.md)
  - LOCK_LOCKTAG
  - LOCKBIT_ON
- Called from (representative examples):
  - [FindLockCycleRecurse](FindLockCycleRecurse.md)

## Notes and Other Information
- Skips relation extension locks as they cannot cause deadlocks by design
- Distinguishes between hard blocks (holding conflicting locks) and soft blocks (waiting ahead in queue)
- Handles hypothetical wait queue reorderings when testing deadlock resolution strategies
- Sets global variable blocking_autovacuum_proc when an autovacuum directly blocks the current process
- Fills deadlockDetails[] array when cycles are detected to provide debugging information
- Uses both regular wait queue traversal and hypothetical reordered queues from TopoSort results
- Critical for accurate deadlock detection in complex locking scenarios involving lock groups

## Simplified Source

```c
static bool FindLockCycleRecurseMember(PGPROC *checkProc,
                                     PGPROC *checkProcLeader,
                                     int depth,
                                     EDGE *softEdges,
                                     int *nSoftEdges) {
    LOCK *lock = checkProc->waitLock;

    // Skip relation extension locks - they can't cause deadlocks
    if (LOCK_LOCKTAG(*lock) == LOCKTAG_RELATION_EXTEND)
        return false;

    LockMethod lockMethodTable = GetLocksMethodTable(lock);
    int conflictMask = lockMethodTable->conflictTab[checkProc->waitLockMode];

    // Check for hard blocks: processes already holding conflicting locks
    dlist_iter proclock_iter;
    dlist_foreach(proclock_iter, &lock->procLocks) {
        PROCLOCK *proclock = dlist_container(PROCLOCK, lockLink, proclock_iter.cur);
        PGPROC *proc = proclock->tag.myProc;
        PGPROC *leader = proc->lockGroupLeader ? proc->lockGroupLeader : proc;

        // Skip processes in same lock group
        if (leader == checkProcLeader)
            continue;

        // Check for lock mode conflicts
        for (int lm = 1; lm <= lockMethodTable->numLockModes; lm++) {
            if ((proclock->holdMask & LOCKBIT_ON(lm)) &&
                (conflictMask & LOCKBIT_ON(lm))) {

                // Found hard conflict - recurse to check for cycle
                if (FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges)) {
                    // Record deadlock details
                    DEADLOCK_INFO *info = &deadlockDetails[depth];
                    info->locktag = lock->tag;
                    info->lockmode = checkProc->waitLockMode;
                    info->pid = checkProc->pid;
                    return true;
                }

                // Check for blocking autovacuum process
                if (checkProc == MyProc && proc->statusFlags & PROC_IS_AUTOVACUUM)
                    blocking_autovacuum_proc = proc;
                break;
            }
        }
    }

    // Check for soft blocks: processes ahead in wait queue with conflicting requests
    bool found_wait_order = false;
    for (int i = 0; i < nWaitOrders; i++) {
        if (waitOrders[i].lock == lock) {
            // Use hypothetical wait queue order
            PGPROC **procs = waitOrders[i].procs;
            int queue_size = waitOrders[i].nProcs;

            for (int j = 0; j < queue_size; j++) {
                PGPROC *proc = procs[j];
                PGPROC *leader = proc->lockGroupLeader ? proc->lockGroupLeader : proc;

                if (leader == checkProcLeader)
                    break;

                if ((LOCKBIT_ON(proc->waitLockMode) & conflictMask) != 0) {
                    if (FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges)) {
                        // Record deadlock and soft edge
                        DEADLOCK_INFO *info = &deadlockDetails[depth];
                        info->locktag = lock->tag;
                        info->lockmode = checkProc->waitLockMode;
                        info->pid = checkProc->pid;

                        softEdges[*nSoftEdges].waiter = checkProcLeader;
                        softEdges[*nSoftEdges].blocker = leader;
                        softEdges[*nSoftEdges].lock = lock;
                        (*nSoftEdges)++;
                        return true;
                    }
                }
            }
            found_wait_order = true;
            break;
        }
    }

    if (!found_wait_order) {
        // Use actual lock wait queue
        PGPROC *lastGroupMember = checkProc;
        if (checkProcLeader != checkProc) {
            // Find last member of lock group in wait queue
            dlist_iter proc_iter;
            dclist_foreach(proc_iter, &lock->waitProcs) {
                PGPROC *proc = dlist_container(PGPROC, links, proc_iter.cur);
                if (proc->lockGroupLeader == checkProcLeader)
                    lastGroupMember = proc;
            }
        }

        // Check processes ahead of our group in wait queue
        dlist_iter proc_iter;
        dclist_foreach(proc_iter, &lock->waitProcs) {
            PGPROC *proc = dlist_container(PGPROC, links, proc_iter.cur);
            PGPROC *leader = proc->lockGroupLeader ? proc->lockGroupLeader : proc;

            if (proc == lastGroupMember)
                break;

            if ((LOCKBIT_ON(proc->waitLockMode) & conflictMask) != 0 &&
                leader != checkProcLeader) {

                if (FindLockCycleRecurse(proc, depth + 1, softEdges, nSoftEdges)) {
                    // Record deadlock and soft edge
                    DEADLOCK_INFO *info = &deadlockDetails[depth];
                    info->locktag = lock->tag;
                    info->lockmode = checkProc->waitLockMode;
                    info->pid = checkProc->pid;

                    softEdges[*nSoftEdges].waiter = checkProcLeader;
                    softEdges[*nSoftEdges].blocker = leader;
                    softEdges[*nSoftEdges].lock = lock;
                    (*nSoftEdges)++;
                    return true;
                }
            }
        }
    }

    return false;
}
```
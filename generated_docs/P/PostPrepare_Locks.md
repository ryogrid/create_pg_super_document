# PostPrepare_Locks

## Location
[src/backend/storage/lmgr/lock.c:3400-3583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3400-L3583)

## Overview
PostPrepare_Locks transfers ownership of transaction-level locks from the current process to a dummy PGPROC associated with a prepared transaction after successful PREPARE.

## Definition
```c
void PostPrepare_Locks(TransactionId xid)
```

## Detailed Description
This function is called after a successful PREPARE TRANSACTION to transfer lock ownership from the current backend process to a dummy PGPROC structure that represents the prepared transaction. It performs two main phases:

**Phase 1 - Local Lock Cleanup:**
- Scans the local lock table (LOCALLOCK entries) to identify transaction-level locks
- Marks the release mask in corresponding PROCLOCK entries to indicate which lock modes need to be transferred
- Removes LOCALLOCK entries to clean up the backend's local state
- Skips session-level locks and VXID locks

**Phase 2 - Lock Ownership Transfer:**
- Iterates through each lock partition to find PROCLOCK entries owned by the current process
- Uses hash_update_hash_key() to reassign PROCLOCK ownership from the current process to the dummy PGPROC
- Updates the proclock's chain linkage to move it from the current process to the prepared transaction
- Maintains all lock state information while changing ownership

The entire operation runs in a critical section to ensure atomicity and consistency.

## Parameters / Member Variables
- `xid`: TransactionId of the prepared transaction that will own the transferred locks

## Dependencies
- Functions called/Symbols referenced:
  - [TwoPhaseGetDummyProc](../T/TwoPhaseGetDummyProc.md)
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [RemoveLocalLock](../R/RemoveLocalLock.md)
  - LOCKBIT_ON
  - LockHashPartitionLockByIndex
  - dlist_foreach_modify
  - dlist_container
  - [hash_update_hash_key](../h/hash_update_hash_key.md)
  - [dlist_push_tail](../d/dlist_push_tail.md)
  - START_CRIT_SECTION/END_CRIT_SECTION
- Called from (representative examples):
  - [PrepareTransaction](PrepareTransaction.md)

## Notes and Other Information
- The function operates within a critical section to prevent interruption during lock transfer
- Lock group leaders cannot be prepared - only individual processes or group leaders themselves
- Virtual transaction (VXID) locks are excluded from transfer as they are not meaningful after restart
- The function assumes that fast-path locks were already moved to the main table during AtPrepare_Locks()
- [PROCLOCK](PROCLOCK.md) hash keys are updated in-place rather than creating new entries to avoid out-of-memory issues
- After transfer, the dummy PGPROC will hold all the locks until COMMIT PREPARED or ROLLBACK PREPARED
- Dangling pointers in the transaction's resource owner are acceptable since resowner.c doesn't free locks at toplevel commit/abort
- The releaseMask and holdMask should be equal for all locks being transferred (no partial releases)

## Simplified Source

```c
void PostPrepare_Locks(TransactionId xid)
{
    PGPROC *newproc = TwoPhaseGetDummyProc(xid, false);
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock;
    LOCK *lock;
    PROCLOCK *proclock;
    PROCLOCKTAG proclocktag;
    int partition;

    // Lock group followers cannot be prepared
    Assert(MyProc->lockGroupLeader == NULL || MyProc->lockGroupLeader == MyProc);

    START_CRIT_SECTION();

    // Phase 1: Clean up local lock table entries
    hash_seq_init(&status, LockMethodLocalHash);
    while ((locallock = (LOCALLOCK *) hash_seq_search(&status)) != NULL)
    {
        LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
        bool haveSessionLock = false;
        bool haveXactLock = false;
        int i;

        // Skip invalid entries
        if (locallock->proclock == NULL || locallock->lock == NULL)
        {
            Assert(locallock->nLocks == 0);
            RemoveLocalLock(locallock);
            continue;
        }

        // Skip virtual transaction locks
        if (locallock->tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION)
            continue;

        // Check what level of locks we hold
        for (i = locallock->numLockOwners - 1; i >= 0; i--)
        {
            if (lockOwners[i].owner == NULL)
                haveSessionLock = true;
            else
                haveXactLock = true;
        }

        // Skip if only session lock
        if (!haveXactLock)
            continue;

        // Cannot have both session and transaction locks
        if (haveSessionLock)
            ereport(PANIC, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                errmsg("cannot PREPARE while holding both session-level and transaction-level locks on the same object")));

        // Mark for release and remove local entry
        if (locallock->nLocks > 0)
            locallock->proclock->releaseMask |= LOCKBIT_ON(locallock->tag.mode);

        RemoveLocalLock(locallock);
    }

    // Phase 2: Transfer ownership of proclocks to dummy proc
    for (partition = 0; partition < NUM_LOCK_PARTITIONS; partition++)
    {
        LWLock *partitionLock = LockHashPartitionLockByIndex(partition);
        dlist_head *procLocks = &(MyProc->myProcLocks[partition]);
        dlist_mutable_iter proclock_iter;

        // Skip empty partitions
        if (dlist_is_empty(procLocks))
            continue;

        LWLockAcquire(partitionLock, LW_EXCLUSIVE);

        dlist_foreach_modify(proclock_iter, procLocks)
        {
            proclock = dlist_container(PROCLOCK, procLink, proclock_iter.cur);
            Assert(proclock->tag.myProc == MyProc);

            lock = proclock->tag.myLock;

            // Skip virtual transaction locks
            if (lock->tag.locktag_type == LOCKTAG_VIRTUALTRANSACTION)
                continue;

            // Skip if nothing to release (session locks)
            if (proclock->releaseMask == 0)
                continue;

            // Should be releasing all held locks
            if (proclock->releaseMask != proclock->holdMask)
                elog(PANIC, "we seem to have dropped a bit somewhere");

            // Remove from current proc's list
            dlist_delete(&proclock->procLink);

            // Create new hash key for dummy proc
            proclocktag.myLock = lock;
            proclocktag.myProc = newproc;

            // Update group leader
            Assert(proclock->groupLeader == proclock->tag.myProc);
            proclock->groupLeader = newproc;

            // Update hash table to transfer ownership
            if (!hash_update_hash_key(LockMethodProcLockHash, proclock, &proclocktag))
                elog(PANIC, "duplicate entry found while reassigning a prepared transaction's locks");

            // Add to dummy proc's list
            dlist_push_tail(&newproc->myProcLocks[partition], &proclock->procLink);
        }

        LWLockRelease(partitionLock);
    }

    END_CRIT_SECTION();
}
```
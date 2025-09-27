# GetRunningTransactionLocks

## Location
[src/backend/storage/lmgr/lock.c:3988-4069](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3988-L4069)

## Overview
GetRunningTransactionLocks returns a list of currently held AccessExclusiveLocks on relations for use by LogStandbySnapshot in WAL-based replication scenarios.

## Definition

```c
structure.
	 *
	 * Must grab LWLocks in partition-number order to avoid LWLock deadlock.
	 */
	for (i = 0;
```
## Detailed Description
This function scans the lock manager's shared lock table to identify all currently granted AccessExclusiveLocks on relations. It's specifically designed for use by the standby snapshot logging mechanism in PostgreSQL's streaming replication system.

The function operates by:
1. **Lock table acquisition**: Takes shared locks on all lock table partitions to ensure consistent data
2. **Space allocation**: Allocates memory for the worst-case scenario (all locks being AccessExclusiveLocks)
3. **Lock filtering**: Scans all PROCLOCKs and identifies those that hold AccessExclusiveLocks on relations
4. **Transaction validation**: Filters out locks from transactions that have already committed but not yet released locks
5. **Data collection**: Extracts transaction ID, database OID, and relation OID for each qualifying lock

The function is optimized for the specific case of AccessExclusiveLocks, which can only have one holder, avoiding the complexity of duplicate lock handling that would be required for shared lock types.

## Parameters / Member Variables
- : Output parameter that receives the number of AccessExclusiveLocks found
- : Transaction ID of the lock holder
- : Database OID where the locked relation exists
- : Relation OID of the locked object

## Dependencies
- Functions called/Symbols referenced:
  - ,  - Lock table partition management
  -  - Count total PROCLOCKs in the hash table
  - ,  - [Hash](../H/Hash.md) table iteration
  -  - Memory allocation for result array
  -  - Access individual lock partitions
  -  - Lock mode bit manipulation
  -  - Transaction ID validation

- Called from (representative examples):
  -  - WAL logging for standby server consistency

## Notes and Other Information
- Only considers AccessExclusiveLocks on relations (LOCKTAG_RELATION), ignoring other lock types and targets
- Takes a conservative approach by locking all partitions simultaneously, though the comment suggests optimization possibilities using reference counting
- Filters out locks from transactions that have committed but not yet released locks by checking transaction ID validity
- Uses the same lock acquisition ordering as other lock table scanning functions to avoid deadlocks
- Memory allocation is deliberately oversized for simplicity and performance, allocating space for all possible locks
- The function guarantees that AccessExclusiveLocks are never duplicated in the result since they can only have one holder
- Critical for maintaining consistency in streaming replication by ensuring standby servers are aware of exclusive locks that could affect recovery

## Simplified Source

```c
// Simplified version of GetRunningTransactionLocks
xl_standby_lock *
GetRunningTransactionLocks(int *nlocks) {
    xl_standby_lock *accessExclusiveLocks;
    PROCLOCK *proclock;
    HASH_SEQ_STATUS seqstat;
    int index = 0;
    int total_entries;

    // Step 1: Lock all partitions to ensure consistent view
    for (int i = 0; i < NUM_LOCK_PARTITIONS; i++) {
        LWLockAcquire(LockHashPartitionLockByIndex(i), LW_SHARED);
    }

    // Step 2: Count entries and allocate result array
    total_entries = hash_get_num_entries(LockMethodProcLockHash);
    accessExclusiveLocks = palloc(total_entries * sizeof(xl_standby_lock));

    // Step 3: Scan lock table for AccessExclusiveLocks on relations
    hash_seq_init(&seqstat, LockMethodProcLockHash);
    while ((proclock = (PROCLOCK *) hash_seq_search(&seqstat))) {

        // Check if this is an AccessExclusiveLock on a relation
        if ((proclock->holdMask & LOCKBIT_ON(AccessExclusiveLock)) &&
            proclock->tag.myLock->tag.locktag_type == LOCKTAG_RELATION) {

            PGPROC *proc = proclock->tag.myProc;
            LOCK *lock = proclock->tag.myLock;
            TransactionId xid = proc->xid;

            // Skip locks from already-committed transactions
            if (!TransactionIdIsValid(xid)) {
                continue;
            }

            // Record the lock information
            accessExclusiveLocks[index].xid = xid;
            accessExclusiveLocks[index].dbOid = lock->tag.locktag_field1;
            accessExclusiveLocks[index].relOid = lock->tag.locktag_field2;
            index++;
        }
    }

    // Step 4: Release all partition locks in reverse order
    for (int i = NUM_LOCK_PARTITIONS; --i >= 0;) {
        LWLockRelease(LockHashPartitionLockByIndex(i));
    }

    *nlocks = index;
    return accessExclusiveLocks;
}
```

Key simplifications made:
- Consolidated variable declarations and initialization
- Added step-by-step comments for main algorithm phases
- Simplified loop variable declarations (using C99 style)
- Removed detailed comments about implementation details and alternatives
- Focused on the core logic: lock acquisition, scanning, filtering, and cleanup
- Preserved all essential functionality and error handling
- Maintained the critical ordering requirements for deadlock avoidance
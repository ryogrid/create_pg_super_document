# AtPrepare_Locks

## Location
[src/backend/storage/lmgr/lock.c:3304-3399](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L3304-L3399)

## Overview
AtPrepare_Locks performs preparatory work for PREPARE TRANSACTION by creating 2PC state file records for all transaction-level locks currently held.

## Definition
```c
void AtPrepare_Locks(void)
```

## Detailed Description
This function is called during PREPARE TRANSACTION processing to serialize all transaction-level locks into the two-phase commit state file. It performs the following key operations:

1. **Validation**: Calls CheckForSessionAndXactLocks() to ensure there are no conflicts between session-level and transaction-level locks on the same object
2. **Lock enumeration**: Scans the local lock table (LockMethodLocalHash) to find all currently held locks
3. **Filtering**: Excludes session-level locks and virtual transaction (VXID) locks from serialization
4. **Fast-path handling**: Moves any fast-path locks to the main lock table to ensure they can be properly managed during recovery
5. **2PC record creation**: Creates TwoPhaseLockRecord entries for each qualifying lock and registers them via RegisterTwoPhaseRecord()

The function ensures that only transaction-level locks are preserved across the PREPARE/COMMIT PREPARED boundary, while maintaining proper reference counting for strong locks.

## Parameters / Member Variables
This function takes no parameters and operates on global lock state.

## Dependencies
- Functions called/Symbols referenced:
  - CheckForSessionAndXactLocks
  - [hash_seq_init](../h/hash_seq_init.md)
  - [hash_seq_search](../h/hash_seq_search.md)
  - [FastPathGetRelationLockEntry](../F/FastPathGetRelationLockEntry.md)
  - [RegisterTwoPhaseRecord](../R/RegisterTwoPhaseRecord.md)
  - [TwoPhaseLockRecord](../T/TwoPhaseLockRecord.md)
  - TWOPHASE_RM_LOCK_ID
- Called from (representative examples):
  - [PrepareTransaction](../P/PrepareTransaction.md)

## Notes and Other Information
- [Session](../S/Session.md)-level locks are completely ignored and not transferred to the prepared transaction
- Virtual transaction (VXID) locks are excluded as they are not meaningful after a database restart
- Fast-path locks are converted to regular lock table entries to ensure proper 2PC handling
- The holdsStrongLockCount flag is cleared to prevent premature strong lock count decrements
- Each qualifying lock generates a TwoPhaseLockRecord that will be processed during COMMIT/ROLLBACK PREPARED
- This function is part of the two-phase commit protocol implementation for maintaining lock consistency across transaction boundaries

## Simplified Source

```c
// Simplified version of AtPrepare_Locks
void AtPrepare_Locks(void) {
    HASH_SEQ_STATUS status;
    LOCALLOCK *locallock;

    // Verify no conflicts between session and transaction level locks
    CheckForSessionAndXactLocks();

    // Scan all local locks
    hash_seq_init(&status, LockMethodLocalHash);

    while ((locallock = (LOCALLOCK *) hash_seq_search(&status)) != NULL) {
        TwoPhaseLockRecord record;
        LOCALLOCKOWNER *lockOwners = locallock->lockOwners;
        bool haveSessionLock;
        bool haveXactLock;
        int i;

        // Skip VXID locks - not meaningful after restart
        if (locallock->tag.lock.locktag_type == LOCKTAG_VIRTUALTRANSACTION)
            continue;

        // Skip if we don't actually hold the lock
        if (locallock->nLocks <= 0)
            continue;

        // Check if we hold this lock at session or transaction level
        haveSessionLock = haveXactLock = false;
        for (i = locallock->numLockOwners - 1; i >= 0; i--) {
            if (lockOwners[i].owner == NULL)
                haveSessionLock = true;
            else
                haveXactLock = true;
        }

        // Only handle transaction-level locks
        if (!haveXactLock)
            continue;

        // Error if holding both session and transaction level
        if (haveSessionLock)
            ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                           errmsg("cannot PREPARE while holding both session-level and transaction-level locks on the same object")));

        // Move fast-path locks to main table if needed
        if (locallock->proclock == NULL) {
            locallock->proclock = FastPathGetRelationLockEntry(locallock);
            locallock->lock = locallock->proclock->tag.myLock;
        }

        // Prevent strong lock count release during prepare
        locallock->holdsStrongLockCount = false;

        // Create 2PC record for this lock
        memcpy(&(record.locktag), &(locallock->tag.lock), sizeof(LOCKTAG));
        record.lockmode = locallock->tag.mode;

        RegisterTwoPhaseRecord(TWOPHASE_RM_LOCK_ID, 0,
                              &record, sizeof(TwoPhaseLockRecord));
    }
}
```

Key simplifications made:
- Preserved the essential 2PC lock serialization logic
- Maintained filtering of VXID and session-level locks
- Kept the fast-path lock handling for proper 2PC support
- Focused on the core lock enumeration and record creation
- Retained error checking for lock level conflicts
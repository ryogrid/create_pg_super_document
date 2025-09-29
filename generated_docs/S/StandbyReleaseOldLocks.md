# StandbyReleaseOldLocks

## Location
[src/backend/storage/ipc/standby.c:1126-1158](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1126-L1158)

## Overview
StandbyReleaseOldLocks selectively releases standby locks held by top-level transaction IDs that are no longer running and are not prepared transactions.

## Definition

```c
void
StandbyReleaseOldLocks(TransactionId oldxid)
```
## Detailed Description
This function performs selective cleanup of recovery locks by releasing locks held by transactions that meet specific criteria: they must be older than the specified transaction ID, must not be running, and must not be prepared transactions. The function uses transaction ID precedence comparison to determine which locks are eligible for release.

The function iterates through all entries in the RecoveryLockXidHash and applies two key filters: it skips prepared transactions (using StandbyTransactionIdIsPrepared) and skips transactions that are not older than the specified oldxid (using TransactionIdPrecedes). This selective approach ensures that only appropriate locks are released while maintaining consistency for active and prepared transactions.

## Parameters / Member Variables
- : The transaction ID threshold; only locks from transactions preceding this ID will be released

## Dependencies
- Functions called/Symbols referenced:
  - [hash_seq_init](../h/hash_seq_init.md) (initialize hash table sequential scan)
  - [hash_seq_search](../h/hash_seq_search.md) (iterate through hash table entries)
  - [StandbyTransactionIdIsPrepared](StandbyTransactionIdIsPrepared.md) (check if transaction is prepared)
  - [TransactionIdPrecedes](../T/TransactionIdPrecedes.md) (compare transaction ID precedence)
  - [StandbyReleaseXidEntryLocks](StandbyReleaseXidEntryLocks.md) (release locks for individual transaction)
  - [hash_search](../h/hash_search.md) (remove entries from hash table with HASH_REMOVE)
- Data structures used:
  - [HASH_SEQ_STATUS](../H/HASH_SEQ_STATUS.md)
  - [RecoveryLockXidEntry](../R/RecoveryLockXidEntry.md)
  - RecoveryLockXidHash
- Called from (representative examples):
  - [ProcArrayApplyRecoveryInfo](../P/ProcArrayApplyRecoveryInfo.md) (src/backend/storage/ipc/procarray.c:1084)

## Notes and Other Information
- This function implements selective lock cleanup based on transaction age and status
- Prepared transactions are explicitly preserved regardless of their age
- Uses PostgreSQL's transaction ID arithmetic to handle wraparound correctly
- The function ensures that only non-running, non-prepared, old transactions have their locks released
- Critical for maintaining proper lock state during long-running recovery operations
- Located in src/backend/storage/ipc/standby.c:1126-1158

## Simplified Source

```c
// Simplified version of StandbyReleaseOldLocks
void StandbyReleaseOldLocks(TransactionId oldxid) {
    HASH_SEQ_STATUS status;
    RecoveryLockXidEntry *entry;

    // Iterate through all recovery lock entries
    hash_seq_init(&status, RecoveryLockXidHash);
    while ((entry = hash_seq_search(&status))) {
        // Skip prepared transactions - they need to stay locked
        if (StandbyTransactionIdIsPrepared(entry->xid))
            continue;

        // Skip transactions that are not older than the threshold
        if (!TransactionIdPrecedes(entry->xid, oldxid))
            continue;

        // Release all locks held by this old, non-prepared transaction
        StandbyReleaseXidEntryLocks(entry);

        // Remove the entry from the hash table
        hash_search(RecoveryLockXidHash, entry, HASH_REMOVE, NULL);
    }
}
```

Key simplifications made:
- Removed Assert statement for clarity
- Added explanatory comments for each major logic step
- Simplified the iteration logic description
- Condensed the two filter conditions with clear explanations
- Combined lock release and hash removal as final cleanup step
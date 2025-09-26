# StandbyReleaseOldLocks

## Location
src/backend/storage/ipc/standby.c: 1126 - 1158

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
  - hash_seq_init (initialize hash table sequential scan)
  - hash_seq_search (iterate through hash table entries)
  - StandbyTransactionIdIsPrepared (check if transaction is prepared)
  - TransactionIdPrecedes (compare transaction ID precedence)
  - StandbyReleaseXidEntryLocks (release locks for individual transaction)
  - hash_search (remove entries from hash table with HASH_REMOVE)
- Data structures used:
  - HASH_SEQ_STATUS
  - RecoveryLockXidEntry
  - RecoveryLockXidHash
- Called from (representative examples):
  - ProcArrayApplyRecoveryInfo (src/backend/storage/ipc/procarray.c:1084)

## Notes and Other Information
- This function implements selective lock cleanup based on transaction age and status
- Prepared transactions are explicitly preserved regardless of their age
- Uses PostgreSQL's transaction ID arithmetic to handle wraparound correctly
- The function ensures that only non-running, non-prepared, old transactions have their locks released
- Critical for maintaining proper lock state during long-running recovery operations
- Located in src/backend/storage/ipc/standby.c:1126-1158
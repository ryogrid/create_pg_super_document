# StandbyReleaseLocks

## Location
src/backend/storage/ipc/standby.c: 1067 - 1090

## Overview
StandbyReleaseLocks is a static function that releases AccessExclusive locks for a specific transaction ID or all locks if InvalidXid is provided during recovery cleanup.

## Definition
static void StandbyReleaseLocks(TransactionId xid)

## Detailed Description
This function serves as a conditional lock release mechanism during WAL replay in hot standby mode. When provided with a valid transaction ID, it searches the RecoveryLockXidHash for the corresponding entry and releases all locks held by that specific transaction using StandbyReleaseXidEntryLocks, then removes the transaction's entry from the hash table. When called with InvalidXid, it delegates to StandbyReleaseAllLocks to release all recovery locks. This function is typically called when a transaction commits or aborts during recovery, ensuring that locks are properly cleaned up to allow standby queries to proceed.

## Parameters / Member Variables
- : Transaction ID whose locks should be released, or InvalidXid to release all locks

## Dependencies
- Functions called/Symbols referenced:
  - TransactionIdIsValid
  - [hash_search](../h/hash_search.md)
  - [StandbyReleaseXidEntryLocks](StandbyReleaseXidEntryLocks.md)
  - StandbyReleaseAllLocks
  - [RecoveryLockXidEntry](../R/RecoveryLockXidEntry.md)
  - HASH_FIND
  - HASH_REMOVE
- Called from (representative examples):
  - StandbyReleaseLockTree (src/backend/storage/ipc/standby.c:1095)
  - StandbyReleaseLockTree (src/backend/storage/ipc/standby.c:1098)

## Notes and Other Information
- Static function not exposed outside standby.c
- Provides both selective (specific XID) and bulk (all locks) release functionality
- Part of the transaction cleanup process during recovery
- Ensures proper cleanup of both individual lock entries and transaction hash entries
- Critical for maintaining lock consistency and preventing lock leaks during WAL replay
- Works in conjunction with the recovery lock tracking system to manage lock state
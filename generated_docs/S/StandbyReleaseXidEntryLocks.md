# StandbyReleaseXidEntryLocks

## Location
src/backend/storage/ipc/standby.c: 1034 - 1066

## Overview
StandbyReleaseXidEntryLocks is a static helper function that releases all AccessExclusive locks associated with a specific transaction during recovery cleanup in hot standby mode.

## Definition
static void StandbyReleaseXidEntryLocks(RecoveryLockXidEntry *xidentry)

## Detailed Description
This internal function iterates through all locks held by a specific transaction during recovery and releases them systematically. It walks through the linked list of RecoveryLockEntry structures associated with a given RecoveryLockXidEntry, releasing each AccessExclusive lock through the lock manager and removing the corresponding hash table entries. The function includes error checking to ensure that locks being released are still tracked by the lock manager, logging warnings if inconsistencies are detected. This is a critical component of the cleanup process when transactions commit or abort during WAL replay.

## Parameters / Member Variables
- : Pointer to RecoveryLockXidEntry containing the linked list of locks held by a specific transaction

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_RELATION
  - [LockRelease](../L/LockRelease.md)
  - [hash_search](../h/hash_search.md)
  - [RecoveryLockEntry](../R/RecoveryLockEntry.md)
  - LOCKTAG
  - AccessExclusiveLock
  - HASH_REMOVE
- Called from (representative examples):
  - [StandbyReleaseLocks](StandbyReleaseLocks.md) (src/backend/storage/ipc/standby.c:1075)
  - StandbyReleaseAllLocks (src/backend/storage/ipc/standby.c:1115)
  - StandbyReleaseOldLocks (src/backend/storage/ipc/standby.c:1145)

## Notes and Other Information
- Static function not exposed outside standby.c
- Includes paranoid cleanup by setting xidentry->head to NULL after releasing all locks  
- Logs detailed DEBUG4 messages for each lock being released
- Contains assertion checks to detect inconsistencies between hash tables and lock manager
- Part of the recovery lock management system that maintains consistency during WAL replay
- Essential for preventing lock leaks when transactions complete during recovery
# StandbyReleaseXidEntryLocks

## Location
[src/backend/storage/ipc/standby.c:1034-1066](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/standby.c#L1034-L1066)

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
  - [LOCKTAG](../L/LOCKTAG.md)
  - AccessExclusiveLock
  - HASH_REMOVE
- Called from (representative examples):
  - [StandbyReleaseLocks](StandbyReleaseLocks.md) (src/backend/storage/ipc/standby.c:1075)
  - [StandbyReleaseAllLocks](StandbyReleaseAllLocks.md) (src/backend/storage/ipc/standby.c:1115)
  - [StandbyReleaseOldLocks](StandbyReleaseOldLocks.md) (src/backend/storage/ipc/standby.c:1145)

## Notes and Other Information
- Static function not exposed outside standby.c
- Includes paranoid cleanup by setting xidentry->head to NULL after releasing all locks
- Logs detailed DEBUG4 messages for each lock being released
- Contains assertion checks to detect inconsistencies between hash tables and lock manager
- Part of the recovery lock management system that maintains consistency during WAL replay
- Essential for preventing lock leaks when transactions complete during recovery

## Simplified Source

```c
// Release all locks held by a transaction during recovery
static void StandbyReleaseXidEntryLocks(RecoveryLockXidEntry *xidentry)
{
    RecoveryLockEntry *entry, *next;

    // Walk through all locks held by this transaction
    for (entry = xidentry->head; entry != NULL; entry = next) {
        LOCKTAG locktag;

        // Log the lock being released
        elog(DEBUG4, "releasing recovery lock: xid %u db %u rel %u",
             entry->key.xid, entry->key.dbOid, entry->key.relOid);

        // Set up lock tag for this relation
        SET_LOCKTAG_RELATION(locktag, entry->key.dbOid, entry->key.relOid);

        // Release the AccessExclusive lock
        if (!LockRelease(&locktag, AccessExclusiveLock, true)) {
            elog(LOG, "RecoveryLockHash contains entry for lock no longer recorded by lock manager: "
                      "xid %u database %u relation %u",
                 entry->key.xid, entry->key.dbOid, entry->key.relOid);
            Assert(false);
        }

        // Remove from hash table and move to next
        next = entry->next;
        hash_search(RecoveryLockHash, entry, HASH_REMOVE, NULL);
    }

    // Clear the head pointer for safety
    xidentry->head = NULL;
}
```
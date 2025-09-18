# UnlockPage

## Location
src/backend/storage/lmgr/lmgr.c: 538 - 557

## Overview
Release a previously acquired page-level lock on a specific page within a relation.

## Definition
```c
void UnlockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
```

## Detailed Description
UnlockPage is a PostgreSQL locking function that releases a previously acquired page-level lock on a specific block within a relation. It constructs the same lock tag that was used during lock acquisition using the relations database ID, relation ID, and block number, then releases the lock using the specified lock mode through the lower-level LockRelease function. This function is the complement to LockPage and ConditionalLockPage operations.

## Parameters / Member Variables
- `relation`: The relation (table/index) containing the page whose lock should be released
- `blkno`: The block number of the specific page within the relation to unlock
- `lockmode`: The type of lock to release (must match the mode used during acquisition)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_PAGE
  - LockRelease
  - LOCKTAG
- Called from (representative examples):
  - ginInsertCleanup (multiple calls)
  - XLTW_Oper

## Notes and Other Information
- This function must be called with the same relation, block number, and lock mode that were used to acquire the lock
- The function passes false for the sessionLock parameter to LockRelease, indicating it is not a session-level lock
- Proper lock/unlock pairing is critical for avoiding deadlocks and ensuring correct concurrency control
- Used primarily by index access methods as the counterpart to page-level lock acquisition functions
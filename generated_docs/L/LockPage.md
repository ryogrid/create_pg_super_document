# LockPage

## Location
src/backend/storage/lmgr/lmgr.c: 503 - 521

## Overview
Obtain a page-level lock for individual pages within a relation, primarily used by index access methods to lock specific index pages.

## Definition
```c
void LockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
```

## Detailed Description
LockPage is a PostgreSQL locking function that acquires a page-level lock on a specific block within a relation. The function is currently used by some index access methods to lock individual index pages, ensuring proper concurrency control at the page level. It constructs a lock tag using the relations database ID, relation ID, and block number, then acquires the lock using the specified lock mode through the lower-level LockAcquire function.

## Parameters / Member Variables
- `relation`: The relation (table/index) containing the page to be locked
- `blkno`: The block number of the specific page within the relation to lock
- `lockmode`: The type of lock to acquire (e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_PAGE
  - [LockAcquire](LockAcquire.md)
  - LOCKTAG
- Called from (representative examples):
  - [ginInsertCleanup](../g/ginInsertCleanup.md)
  - [XLTW_Oper](../X/XLTW_Oper.md)

## Notes and Other Information
- This function is specifically designed for page-level locking, which is more granular than relation-level locking
- The function uses the relations lockInfo to identify the database and relation for lock tagging
- The function always passes false for both session lock and dontWait parameters to LockAcquire
- Page-level locking is primarily used by index access methods for fine-grained concurrency control
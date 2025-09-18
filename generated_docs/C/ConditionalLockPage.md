# ConditionalLockPage

## Location
src/backend/storage/lmgr/lmgr.c: 522 - 537

## Overview
Attempt to obtain a page-level lock without blocking, returning true if the lock was successfully acquired.

## Definition
```c
bool ConditionalLockPage(Relation relation, BlockNumber blkno, LOCKMODE lockmode)
```

## Detailed Description
ConditionalLockPage is a non-blocking variant of LockPage that attempts to acquire a page-level lock on a specific block within a relation. Unlike LockPage, this function will not wait if the lock is not immediately available. It constructs a lock tag using the relations database ID, relation ID, and block number, then attempts to acquire the lock using the specified lock mode. The function returns true if the lock was successfully acquired, false otherwise.

## Parameters / Member Variables
- `relation`: The relation (table/index) containing the page to be locked
- `blkno`: The block number of the specific page within the relation to lock
- `lockmode`: The type of lock to acquire (e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_PAGE
  - LockAcquire
  - LOCKTAG
  - LOCKACQUIRE_NOT_AVAIL
- Called from (representative examples):
  - ginInsertCleanup
  - XLTW_Oper

## Notes and Other Information
- This is the non-blocking version of LockPage that returns immediately rather than waiting for the lock
- Returns true if the lock was acquired, false if it would have required waiting
- Uses the dontWait parameter (true) in LockAcquire to achieve non-blocking behavior
- Checks the return value against LOCKACQUIRE_NOT_AVAIL to determine success/failure
- Useful in scenarios where the caller can perform alternative actions if the lock is not immediately available
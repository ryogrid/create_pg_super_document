# WaitForLockers

## Location
[src/backend/storage/lmgr/lmgr.c:981-999](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L981-L999)

## Overview
WaitForLockers is a convenience wrapper function that waits until no transaction holds locks that conflict with a single given lock tag at the specified lock mode.

## Definition

```c
void
WaitForLockers(LOCKTAG heaplocktag, LOCKMODE lockmode, bool progress)
```
## Detailed Description
WaitForLockers is a simplified interface to WaitForLockersMultiple for cases where only a single lock tag needs to be waited on. It creates a single-element list containing the provided lock tag and delegates the actual waiting logic to WaitForLockersMultiple. This function is commonly used in index operations where the system needs to ensure no conflicting locks are held before proceeding with operations like index drops or creations.

The function works by obtaining the current list of lock holders that conflict with the given lock tag and lock mode, then waiting on their virtual transaction IDs (VXIDs) until they complete. It does not attempt to acquire the lock itself, only waits for conflicting lock holders to finish.

## Parameters / Member Variables
- `heaplocktag`: The LOCKTAG structure identifying the database object to wait for
- `lockmode`: The LOCKMODE specifying the type of lock mode to check for conflicts
- `progress`: Boolean flag indicating whether to report progress information to the statistics collector
## Dependencies
- Functions called/Symbols referenced:
  - [LOCKTAG](../L/LOCKTAG.md) (data structure)
  - [WaitForLockersMultiple](WaitForLockersMultiple.md) (core waiting logic)
  - list_make1 (create single-element list)
  - [list_free](../l/list_free.md) (cleanup list)
- Called from (representative examples):
  - [index_drop](../i/index_drop.md) (src/backend/catalog/index.c:2272, 2288)
  - [DefineIndex](../D/DefineIndex.md) (src/backend/commands/indexcmds.c:1642, 1689)

## Notes and Other Information
- This is essentially a convenience wrapper that simplifies the interface for single lock tag waiting
- The function is part of the PostgreSQL lock manager subsystem and is crucial for ensuring consistency during DDL operations
- Progress reporting is optional and used by operations that want to provide feedback about waiting status
- The function does not acquire locks itself, only waits for conflicting lock holders to complete
- Located in src/backend/storage/lmgr/lmgr.c:981-999

## Simplified Source

```c
void WaitForLockers(LOCKTAG heaplocktag, LOCKMODE lockmode, bool progress) {
    // Create single-item list with the lock tag
    List *l = list_make1(&heaplocktag);

    // Wait for conflicting lockers using multi-tag function
    WaitForLockersMultiple(l, lockmode, progress);

    // Clean up temporary list
    list_free(l);
}
```
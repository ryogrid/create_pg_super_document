# WaitForLockers

## Location
src/backend/storage/lmgr/lmgr.c: 981 - 999

## Overview
WaitForLockers is a convenience wrapper function that waits until no transaction holds locks that conflict with a single given lock tag at the specified lock mode.

## Definition


## Detailed Description
WaitForLockers is a simplified interface to WaitForLockersMultiple for cases where only a single lock tag needs to be waited on. It creates a single-element list containing the provided lock tag and delegates the actual waiting logic to WaitForLockersMultiple. This function is commonly used in index operations where the system needs to ensure no conflicting locks are held before proceeding with operations like index drops or creations.

The function works by obtaining the current list of lock holders that conflict with the given lock tag and lock mode, then waiting on their virtual transaction IDs (VXIDs) until they complete. It does not attempt to acquire the lock itself, only waits for conflicting lock holders to finish.

## Parameters / Member Variables
- : The LOCKTAG structure identifying the database object to wait for
- : The LOCKMODE specifying the type of lock mode to check for conflicts
- : Boolean flag indicating whether to report progress information to the statistics collector

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG (data structure)
  - WaitForLockersMultiple (core waiting logic)
  - list_make1 (create single-element list)
  - list_free (cleanup list)
- Called from (representative examples):
  - index_drop (src/backend/catalog/index.c:2272, 2288)
  - DefineIndex (src/backend/commands/indexcmds.c:1642, 1689)

## Notes and Other Information
- This is essentially a convenience wrapper that simplifies the interface for single lock tag waiting
- The function is part of the PostgreSQL lock manager subsystem and is crucial for ensuring consistency during DDL operations
- Progress reporting is optional and used by operations that want to provide feedback about waiting status
- The function does not acquire locks itself, only waits for conflicting lock holders to complete
- Located in src/backend/storage/lmgr/lmgr.c:981-999
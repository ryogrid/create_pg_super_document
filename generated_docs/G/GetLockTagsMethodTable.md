# GetLockTagsMethodTable

## Location
src/backend/storage/lmgr/lock.c: 486 - 503

## Overview
GetLockTagsMethodTable retrieves the lock method table associated with a given LOCKTAG by extracting the lock method ID from the tag and returning the corresponding LockMethod structure.

## Definition

```c
LockMethod
GetLockTagsMethodTable(const LOCKTAG *locktag)
```
## Detailed Description
GetLockTagsMethodTable is a companion function to GetLocksMethodTable that works directly with LOCKTAG structures instead of LOCK structures. It extracts the lock method identifier from a LOCKTAG's locktag_lockmethodid field, validates that the ID is within valid bounds, and returns a pointer to the appropriate LockMethod structure from the global LockMethods array.

This function is useful when you have a LOCKTAG but not necessarily a full LOCK structure, such as when analyzing lock requests or performing lock-related queries. Like its counterpart GetLocksMethodTable, it provides safe access to lock method information with bounds checking.

## Parameters / Member Variables
- : Pointer to a LOCKTAG structure containing the lock identifier whose method table is being requested
  - The locktag->locktag_lockmethodid field is cast to LOCKMETHODID and used to index into the LockMethods array

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG (structure type)
  - LOCKMETHODID (type definition for casting)
  - LockMethods (global array of lock method structures)
  - lengthof (macro to get array length)  
  - Assert (assertion macro)
- Called from (representative examples):
  - pg_blocking_pids (src/backend/utils/adt/lockfuncs.c:509)

## Notes and Other Information
- The function includes an assertion to validate that the lock method ID is within the valid range (0 < lockmethodid < lengthof(LockMethods))
- This function is particularly useful in lock monitoring and diagnostic functions where you need to access lock method information from a tag
- The locktag_lockmethodid field is explicitly cast to LOCKMETHODID type for type safety
- Invalid lock method IDs will cause assertion failures in debug builds
- Unlike GetLocksMethodTable which works with full LOCK structures, this function can work with just the lock identification information
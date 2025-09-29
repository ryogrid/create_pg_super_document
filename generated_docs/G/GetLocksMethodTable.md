# GetLocksMethodTable

## Location
[src/backend/storage/lmgr/lock.c:474-485](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lock.c#L474-L485)

## Overview
GetLocksMethodTable retrieves the lock method table associated with a given lock by extracting the lock method ID from the lock and returning the corresponding LockMethod structure.

## Definition

```c
LockMethod
GetLocksMethodTable(const LOCK *lock)
```
## Detailed Description
GetLocksMethodTable is a simple accessor function that extracts the lock method identifier from a LOCK structure and returns the corresponding LockMethod from the global LockMethods array. The function uses the LOCK_LOCKMETHOD macro to extract the lockmethodid field from the lock's tag, validates that the ID is within valid bounds, and returns a pointer to the appropriate LockMethod structure.

This function provides a safe way to access lock method information, ensuring that the lock method ID is valid before dereferencing the LockMethods array. The LockMethod structure contains function pointers and configuration data specific to different locking protocols (e.g., default locks vs. user locks).

## Parameters / Member Variables
- : Pointer to a LOCK structure containing the lock information whose method table is being requested
  - The lock's tag.locktag_lockmethodid field is used to index into the LockMethods array

## Dependencies
- Functions called/Symbols referenced:
  - LOCK_LOCKMETHOD (macro that extracts lockmethodid from lock tag)
  - LOCKMETHODID (type definition)
  - LockMethods (global array of lock method structures)
  - lengthof (macro to get array length)
  - Assert (assertion macro)
- Called from (representative examples):
  - [DeadLockCheck](../D/DeadLockCheck.md) (src/backend/storage/lmgr/deadlock.c:269)
  - [FindLockCycleRecurseMember](../F/FindLockCycleRecurseMember.md) (src/backend/storage/lmgr/deadlock.c:556)

## Notes and Other Information
- The function includes an assertion to validate that the lock method ID is within the valid range (0 < lockmethodid < lengthof(LockMethods))
- This is primarily used by the deadlock detector and other lock management routines that need to access lock method-specific information
- The returned LockMethod contains function pointers for lock conflict checking, tracing flags, and other method-specific configuration
- Invalid lock method IDs will cause assertion failures in debug builds

## Simplified Source
```c
LockMethod GetLocksMethodTable(const LOCK *lock) {
    // Extract lock method ID from the lock's tag
    LOCKMETHODID lockmethodid = LOCK_LOCKMETHOD(*lock);

    // Validate the lock method ID is in valid range
    Assert(0 < lockmethodid && lockmethodid < lengthof(LockMethods));

    // Return the corresponding lock method table
    return LockMethods[lockmethodid];
}
```
# ConditionalLockSharedObject

## Location
[src/backend/storage/lmgr/lmgr.c:1103-1137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1103-L1137)

## Overview
ConditionalLockSharedObject attempts to acquire a lock on a shared database object without blocking, returning true if the lock was successfully acquired or false if it would block.

## Definition
```c
bool ConditionalLockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode)
```

## Detailed Description
This function provides a non-blocking variant of LockSharedObject that attempts to acquire a lock on a shared database object identified by its class, object ID, and sub-object ID. Unlike the blocking version, this function will return immediately if the requested lock cannot be acquired without waiting. The function follows the same invalidation message handling pattern as other locking functions, ensuring cache consistency after lock acquisition.

## Parameters / Member Variables
- `classid`: Object identifier (OID) of the system catalog that contains the object to be locked
- `objid`: Object identifier (OID) of the specific object within the catalog to be locked  
- `objsubid`: Sub-object identifier for component parts of the object (e.g., column number for tables)
- `lockmode`: The type of lock to acquire (from LOCKMODE enum, e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - SET_LOCKTAG_OBJECT
  - [LockAcquireExtended](../L/LockAcquireExtended.md)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md)
  - [MarkLockClear](../M/MarkLockClear.md)
- Types used:
  - [LOCKTAG](../L/LOCKTAG.md)
  - [LOCALLOCK](../L/LOCALLOCK.md)
  - LockAcquireResult
  - LOCKACQUIRE_NOT_AVAIL
  - LOCKACQUIRE_ALREADY_CLEAR
- Called from (representative examples):
  - [EventTriggerOnLogin](../E/EventTriggerOnLogin.md)
  - [XLTW_Oper](../X/XLTW_Oper.md)

## Notes and Other Information
- Returns true if lock was acquired, false if lock would block
- Handles invalidation messages after successful lock acquisition to maintain cache consistency
- Uses the same underlying locking mechanism as blocking variants but with conditional acquisition
- Part of PostgreSQL's advisory locking system for shared objects
- The function is located in src/backend/storage/lmgr/lmgr.c:1103-1137

## Simplified Source

```c
// Simplified version of ConditionalLockSharedObject
bool ConditionalLockSharedObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode) {
    LOCKTAG lock_tag;
    LOCALLOCK *local_lock;
    LockAcquireResult result;

    // Step 1: Set up the lock tag to identify the object
    SET_LOCKTAG_OBJECT(lock_tag, InvalidOid, classid, objid, objsubid);

    // Step 2: Try to acquire lock without blocking
    result = LockAcquireExtended(&lock_tag, lockmode, false, true, true, &local_lock);

    // Step 3: Check if lock acquisition failed due to blocking
    if (result == LOCKACQUIRE_NOT_AVAIL) {
        return false;  // Could not get lock without blocking
    }

    // Step 4: Handle cache invalidation if we got a new lock
    if (result != LOCKACQUIRE_ALREADY_CLEAR) {
        AcceptInvalidationMessages();  // Process any pending cache invalidations
        MarkLockClear(local_lock);     // Mark this lock as processed
    }

    // Step 5: Return success
    return true;
}
```

Key simplifications made:
- Renamed variables for clarity (tag → lock_tag, locallock → local_lock, res → result)
- Added step-by-step comments explaining the main operations
- Removed detailed comments about invalidation logic while preserving the essential flow
- Focused on the main execution path: setup lock tag, try acquire, handle result
- Maintained all critical error handling and cache consistency logic
# ReleaseDeletionLock

## Location
src/backend/catalog/dependency.c: 1528 - 1552

## Overview
ReleaseDeletionLock releases locks acquired by AcquireDeletionLock, providing the complementary unlock operation for object deletion synchronization.

## Definition
```c
void ReleaseDeletionLock(const ObjectAddress *object)
```

## Detailed Description
ReleaseDeletionLock is the companion function to AcquireDeletionLock, responsible for releasing locks after deletion operations complete. The function implements a simplified two-tier unlocking strategy: relations use UnlockRelationOid, while all other objects (including shared objects) use UnlockDatabaseObject. Unlike its acquisition counterpart, this function always releases AccessExclusiveLock regardless of the original lock mode, as lock promotion during concurrent operations means the final lock level is always exclusive.

## Parameters / Member Variables
- `object`: Pointer to ObjectAddress specifying the object to unlock
  - `classId`: OID of the catalog relation, determines unlocking strategy
  - `objectId`: OID of the specific object to unlock
  - `objectSubId`: Sub-object identifier (not used in unlocking decision)

## Dependencies
- Functions called/Symbols referenced:
  - UnlockRelationOid: Releases locks on relation objects
  - UnlockDatabaseObject: Releases locks on database objects
  - AccessExclusiveLock: Lock mode constant for exclusive access
- Called from:
  - findDependentObjects: Dependency analysis function (multiple locations)
  - shdepDropOwned: Shared dependency cleanup (multiple locations)
  - PERFORM_DELETION_CONCURRENT_LOCK: Referenced in header file

## Notes and Other Information
- This is a public function exported from the dependency module
- Simplifies the locking strategy compared to AcquireDeletionLock - treats shared objects the same as regular database objects for unlocking
- Always releases AccessExclusiveLock, reflecting that concurrent operations promote locks to exclusive level
- Does not handle different lock modes since PostgreSQL's lock manager handles lock promotion internally
- Always unlocks the whole object (subId=0) for consistency with acquisition behavior
- Critical for preventing deadlocks and ensuring proper lock cleanup after deletion operations
- Part of the public API for dependency management and object deletion
# AcquireDeletionLock

## Location
src/backend/catalog/dependency.c: 1496 - 1527

## Overview
AcquireDeletionLock acquires appropriate locks for object deletion operations, with specialized handling for different object types and concurrent deletion modes.

## Definition
```c
void AcquireDeletionLock(const ObjectAddress *object, int flags)
```

## Detailed Description
AcquireDeletionLock provides object-type-aware locking for deletion operations. For relations, it uses LockRelationOid with either ShareUpdateExclusiveLock for concurrent index drops or AccessExclusiveLock for standard deletions. For shared objects like authentication members, it uses LockSharedObject. For all other database objects, it uses LockDatabaseObject with AccessExclusiveLock. The function ensures proper synchronization during deletion operations while supporting concurrent deletion modes where appropriate.

## Parameters / Member Variables
- `object`: Pointer to ObjectAddress specifying the object to lock
  - `classId`: OID of the catalog relation, determines locking strategy
  - `objectId`: OID of the specific object to lock
  - `objectSubId`: Sub-object identifier (not used in locking decision)
- `flags`: Deletion behavior flags:
  - PERFORM_DELETION_CONCURRENTLY: Use concurrent locking mode for relations

## Dependencies
- Functions called/Symbols referenced:
  - LockRelationOid: Acquires locks on relation objects
  - LockSharedObject: Acquires locks on shared objects
  - LockDatabaseObject: Acquires locks on database-specific objects
  - ShareUpdateExclusiveLock: Lock mode constant for concurrent operations
  - AccessExclusiveLock: Lock mode constant for exclusive access
  - PERFORM_DELETION_CONCURRENTLY: Flag constant for concurrent deletion mode
- Called from:
  - performDeletion: Single object deletion function
  - performMultipleDeletions: Multiple object deletion function
  - findDependentObjects: Dependency analysis function (multiple locations)
  - shdepDropOwned: Shared dependency cleanup (multiple locations)
  - PERFORM_DELETION_CONCURRENT_LOCK: Referenced in header file

## Notes and Other Information
- This is a public function exported from the dependency module
- Implements a three-tier locking strategy based on object type:
  1. Relations: Special handling with concurrent support
  2. Shared objects: Cross-database objects requiring shared locking
  3. Database objects: Standard database-scoped objects
- For concurrent index drops, uses ShareUpdateExclusiveLock initially, allowing index_drop() to promote the lock later
- Always locks the whole object (subId=0) rather than sub-objects for simplicity
- Critical for ensuring proper concurrency control during deletion operations
- Part of the public API for dependency management and object deletion
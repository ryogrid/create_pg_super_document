# UnlockDatabaseObject

## Location
[src/backend/storage/lmgr/lmgr.c:1059-1078](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1059-L1078)

## Overview
UnlockDatabaseObject releases a previously acquired lock on a database object, providing the counterpart to LockDatabaseObject.

## Definition

```c
void
UnlockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
					 LOCKMODE lockmode)
```
## Detailed Description
UnlockDatabaseObject is the complementary function to LockDatabaseObject, used to release locks that were previously acquired on database objects. It constructs the same LOCKTAG using the provided object identifiers and calls LockRelease to release the lock.

The function is straightforward in its operation: it creates an object lock tag using the same parameters that were used to acquire the lock, then releases it through the standard lock manager. This maintains the symmetry with the lock acquisition functions and ensures proper cleanup of database object locks.

Like its locking counterpart, this function is designed for database-scoped objects and should not be used for shared objects or relations that have their own specialized unlocking mechanisms.

## Parameters / Member Variables
- : The OID of the system catalog that contains the object (must match the classid used when acquiring the lock)
- : The OID of the specific object within that catalog (must match the objid used when acquiring the lock)
- : The sub-object identifier (must match the objsubid used when acquiring the lock)
- : The LOCKMODE that was used when acquiring the lock (must match exactly)

## Dependencies
- Functions called/Symbols referenced:
  - LOCKTAG (data structure for lock identification)
  - SET_LOCKTAG_OBJECT (macro to initialize object lock tag)
  - [LockRelease](../L/LockRelease.md) (core lock release function)
- Called from (representative examples):
  - [ReleaseDeletionLock](../R/ReleaseDeletionLock.md) (src/backend/catalog/dependency.c:1534)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md) (src/backend/catalog/namespace.c:804)
  - [get_object_address](../g/get_object_address.md) (src/backend/catalog/objectaddress.c:1162)

## Notes and Other Information
- Must be called with exactly the same parameters (classid, objid, objsubid, lockmode) that were used to acquire the lock
- Does not perform any cache invalidation processing unlike the lock acquisition functions
- Should only be used for locks acquired via LockDatabaseObject or ConditionalLockDatabaseObject
- Part of the database object locking API alongside LockDatabaseObject and ConditionalLockDatabaseObject
- The lock is scoped to the current database (MyDatabaseId is used in the lock tag)
- Simple wrapper around the core LockRelease functionality with object-specific lock tag construction
- Located in src/backend/storage/lmgr/lmgr.c:1059-1078
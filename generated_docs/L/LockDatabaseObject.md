# LockDatabaseObject

## Location
[src/backend/storage/lmgr/lmgr.c:1000-1023](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1000-L1023)

## Overview
LockDatabaseObject obtains a lock on a general database object within the current database, providing a way to synchronize access to catalog objects and other non-relation database objects.

## Definition

```c
void
LockDatabaseObject(Oid classid, Oid objid, uint16 objsubid,
				   LOCKMODE lockmode)
```
## Detailed Description
LockDatabaseObject is used to acquire locks on general database objects that belong to the current database. It creates a database-specific lock tag using the provided object identifiers and acquires the lock through the standard PostgreSQL lock manager. The function is specifically designed for catalog objects and other database-scoped objects.

Important restrictions apply: this function should not be used for shared objects (like tablespaces) or relations. For relations, the specialized LockRelation family of functions should be used instead, as locks taken via LockDatabaseObject will not conflict with relation-specific locks.

After acquiring the lock, the function calls AcceptInvalidationMessages() to ensure that any cached system catalog information is updated with changes that may have occurred while waiting for the lock.

## Parameters / Member Variables
- : The OID of the system catalog (pg_class entry) that contains the object
- : The OID of the specific object to lock within that catalog
- : A sub-object identifier (typically 0 for whole objects, or column numbers for attributes)
- : The LOCKMODE specifying the type of lock to acquire (e.g., AccessShareLock, ExclusiveLock)

## Dependencies
- Functions called/Symbols referenced:
  - [LOCKTAG](LOCKTAG.md) (data structure for lock identification)
  - SET_LOCKTAG_OBJECT (macro to initialize object lock tag)
  - [LockAcquire](LockAcquire.md) (core lock acquisition function)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (system cache invalidation handling)
- Called from (representative examples):
  - [AcquireDeletionLock](../A/AcquireDeletionLock.md) (src/backend/catalog/dependency.c:1517)
  - [RangeVarGetAndCheckCreationNamespace](../R/RangeVarGetAndCheckCreationNamespace.md) (src/backend/catalog/namespace.c:813)
  - [get_object_address](../g/get_object_address.md) (src/backend/catalog/objectaddress.c:1178)
  - [AddEnumLabel](../A/AddEnumLabel.md) (src/backend/catalog/pg_enum.c:326)
  - [RenameEnumLabel](../R/RenameEnumLabel.md) (src/backend/catalog/pg_enum.c:635)

## Notes and Other Information
- Should NOT be used for shared objects (tablespaces) or relations - use appropriate specialized locking functions instead
- Locks taken this way will not conflict with relation-specific locks from LockRelation functions
- The lock is scoped to the current database (MyDatabaseId is used in the lock tag)
- Always calls AcceptInvalidationMessages() after lock acquisition to maintain cache consistency
- Commonly used for DDL operations on catalog objects like enums, publications, and schemas
- Located in src/backend/storage/lmgr/lmgr.c:1000-1023

## Simplified Source

```c
void LockDatabaseObject(Oid classid, Oid objid, uint16 objsubid, LOCKMODE lockmode) {
    LOCKTAG tag;

    // Build lock tag for the database object
    SET_LOCKTAG_OBJECT(tag, MyDatabaseId, classid, objid, objsubid);

    // Acquire the lock
    LockAcquire(&tag, lockmode, false, false);

    // Ensure system caches are updated after waiting for lock
    AcceptInvalidationMessages();
}
```
# LockSharedObject

## Location
[src/backend/storage/lmgr/lmgr.c:1079-1102](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lmgr.c#L1079-L1102)

## Overview
LockSharedObject obtains a lock on objects that are shared across all databases in a PostgreSQL cluster, such as roles, tablespaces, and replication origins.

## Definition

```c
void
LockSharedObject(Oid classid, Oid objid, uint16 objsubid,
				 LOCKMODE lockmode)
```
## Detailed Description
LockSharedObject is used to acquire locks on objects that are shared across all databases in a PostgreSQL cluster, rather than being scoped to a single database. These include system-wide objects such as roles (users), tablespaces, databases themselves, replication origins, and subscriptions.

The key difference from LockDatabaseObject is that the lock tag is created with InvalidOid as the database ID, indicating that the lock applies cluster-wide rather than being limited to the current database. This ensures that operations on shared objects are properly synchronized across all databases in the cluster.

Like other locking functions in this family, it calls AcceptInvalidationMessages() after acquiring the lock to ensure system catalog caches are up-to-date with any changes that occurred while waiting for the lock.

## Parameters / Member Variables
- : The OID of the system catalog that contains the shared object (e.g., AuthIdRelationId for roles)
- : The OID of the specific shared object to lock
- : A sub-object identifier (typically 0 for whole objects)
- : The LOCKMODE specifying the type of lock to acquire

## Dependencies
- Functions called/Symbols referenced:
  - [LOCKTAG](LOCKTAG.md) (data structure for lock identification)
  - SET_LOCKTAG_OBJECT (macro to initialize object lock tag with InvalidOid for database)
  - [LockAcquire](LockAcquire.md) (core lock acquisition function)
  - [AcceptInvalidationMessages](../A/AcceptInvalidationMessages.md) (system cache invalidation handling)
- Called from (representative examples):
  - [AcquireDeletionLock](../A/AcquireDeletionLock.md) (src/backend/catalog/dependency.c:1512)
  - [get_object_address](../g/get_object_address.md) (src/backend/catalog/objectaddress.c:1175)
  - [shdepLockAndCheckObject](../s/shdepLockAndCheckObject.md) (src/backend/catalog/pg_shdepend.c:1214)
  - [DisableSubscription](../D/DisableSubscription.md) (src/backend/catalog/pg_subscription.c:184)
  - [createdb](../c/createdb.md) (src/backend/commands/dbcommands.c:1485)
  - [DropRole](../D/DropRole.md) (src/backend/commands/user.c:1191)
  - [AddRoleMems](../A/AddRoleMems.md) (src/backend/commands/user.c:1703)
  - [InitPostgres](../I/InitPostgres.md) (src/backend/utils/init/postinit.c:1076)

## Notes and Other Information
- Used specifically for cluster-wide shared objects (roles, tablespaces, databases, replication objects)
- Lock tag uses InvalidOid as database ID to indicate cluster-wide scope
- Complements LockDatabaseObject which is used for database-scoped objects
- Always processes invalidation messages after lock acquisition for cache consistency
- Critical for maintaining consistency of shared catalog operations across the entire cluster
- Commonly used in DDL operations involving shared objects like CREATE/DROP ROLE, CREATE DATABASE, etc.
- Located in src/backend/storage/lmgr/lmgr.c:1079-1102
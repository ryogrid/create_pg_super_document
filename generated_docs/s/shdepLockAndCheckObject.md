# shdepLockAndCheckObject

## Location
[src/backend/catalog/pg_shdepend.c:1211-1275](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/pg_shdepend.c#L1211-L1275)

## Overview
Locks a shared object and verifies it still exists before recording a dependency on it, preventing race conditions with concurrent DROP operations.

## Definition

```c
void
shdepLockAndCheckObject(Oid classId, Oid objectId)
```
## Detailed Description
This function provides essential synchronization for shared dependency tracking by acquiring an AccessShareLock on the target object and then verifying that the object hasn't been concurrently dropped. The function handles different types of shared objects (roles, tablespaces, databases) with appropriate existence checks.

The locking prevents the object from being dropped while a dependency is being recorded, while the existence check ensures that the object wasn't dropped between the time the dependency operation started and when the lock was acquired. If the object is found to be missing, the function raises an error and does not return.

## Parameters / Member Variables
- `classId`: OID of the catalog table containing the object (AuthIdRelationId, TableSpaceRelationId, or DatabaseRelationId)
- `objectId`: OID of the shared object to lock and verify
## Dependencies
- Functions called/Symbols referenced:
  - [LockSharedObject](../L/LockSharedObject.md)
  - SearchSysCacheExists1
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [get_tablespace_name](../g/get_tablespace_name.md)
  - [get_database_name](../g/get_database_name.md)
  - [pfree](../p/pfree.md)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - elog
- Called from (representative examples):
  - [shdepAddDependency](shdepAddDependency.md)
  - [shdepChangeDep](shdepChangeDep.md)
  - [AlterDatabaseSet](../A/AlterDatabaseSet.md)
  - [AlterRoleSet](../A/AlterRoleSet.md)

## Notes and Other Information
- This is a public function (not static) accessible from other source files
- Uses AccessShareLock which allows concurrent reads but prevents drops
- Handles three types of shared objects with different existence verification methods:
  - Roles: Uses SearchSysCacheExists1 with AUTHOID syscache
  - Tablespaces: Uses get_tablespace_name() due to lack of syscache
  - Databases: Uses get_database_name() due to lack of syscache
- Function does not return if the object is found to be missing (ereport with ERROR)
- Critical for preventing orphaned dependency records in pg_shdepend
- Memory management: properly frees allocated strings for tablespace and database names

## Simplified Source

```c
void
shdepLockAndCheckObject(Oid classId, Oid objectId)
{
    // Lock the object to prevent concurrent drops
    LockSharedObject(classId, objectId, 0, AccessShareLock);

    // Verify object still exists after acquiring lock
    switch (classId)
    {
        case AuthIdRelationId:
            // Check role existence via syscache
            if (!SearchSysCacheExists1(AUTHOID, ObjectIdGetDatum(objectId)))
                ereport(ERROR, "role %u was concurrently dropped", objectId);
            break;

        case TableSpaceRelationId:
            {
                // Check tablespace existence (no syscache available)
                char *tablespace = get_tablespace_name(objectId);
                if (tablespace == NULL)
                    ereport(ERROR, "tablespace %u was concurrently dropped", objectId);
                pfree(tablespace);
                break;
            }

        case DatabaseRelationId:
            {
                // Check database existence (no syscache available)
                char *database = get_database_name(objectId);
                if (database == NULL)
                    ereport(ERROR, "database %u was concurrently dropped", objectId);
                pfree(database);
                break;
            }

        default:
            elog(ERROR, "unrecognized shared classId: %u", classId);
    }
}
```
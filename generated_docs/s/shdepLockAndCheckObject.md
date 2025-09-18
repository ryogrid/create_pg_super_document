# shdepLockAndCheckObject

## Location
src/backend/catalog/pg_shdepend.c: 1211 - 1275

## Overview
Locks a shared object and verifies it still exists before recording a dependency on it, preventing race conditions with concurrent DROP operations.

## Definition


## Detailed Description
This function provides essential synchronization for shared dependency tracking by acquiring an AccessShareLock on the target object and then verifying that the object hasn't been concurrently dropped. The function handles different types of shared objects (roles, tablespaces, databases) with appropriate existence checks.

The locking prevents the object from being dropped while a dependency is being recorded, while the existence check ensures that the object wasn't dropped between the time the dependency operation started and when the lock was acquired. If the object is found to be missing, the function raises an error and does not return.

## Parameters / Member Variables
- : OID of the catalog table containing the object (AuthIdRelationId, TableSpaceRelationId, or DatabaseRelationId)
- : OID of the shared object to lock and verify

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
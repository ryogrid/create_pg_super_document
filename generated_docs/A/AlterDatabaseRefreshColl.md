# AlterDatabaseRefreshColl

## Location
src/backend/commands/dbcommands.c: 2501 - 2597

## Overview
AlterDatabaseRefreshColl implements the ALTER DATABASE name REFRESH COLLATION VERSION command, which updates the stored collation version information for a database to match the current system collation version.

## Definition


## Detailed Description
This function handles the SQL command `ALTER DATABASE name REFRESH COLLATION VERSION` which is used to update the collation version stored in the pg_database catalog when the underlying system collation library has been updated. The function:

1. Opens the pg_database catalog table with RowExclusiveLock
2. Searches for the specified database by name
3. Verifies the user has ownership privileges on the database
4. Retrieves the current stored collation version and the database's collation settings
5. Calls get_collation_actual_version() to determine the current system collation version
6. Compares the stored version with the actual version and updates the catalog if they differ
7. Issues appropriate NOTICE messages about version changes
8. Triggers post-alter hooks and returns the database's ObjectAddress

The function ensures that collation version tracking remains accurate after system collation library updates, which is important for detecting potential index corruption due to collation changes.

## Parameters / Member Variables
- `stmt`: Pointer to AlterDatabaseRefreshCollStmt containing the database name to refresh

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - ScanKeyInit 
  - systable_beginscan
  - systable_getnext
  - object_ownercheck
  - aclcheck_error
  - LockTuple/UnlockTuple
  - heap_getattr
  - get_collation_actual_version
  - heap_modify_tuple
  - CatalogTupleUpdate
  - heap_freetuple
  - InvokeObjectPostAlterHook
  - ObjectAddressSet
- Called from (representative examples):
  - standard_ProcessUtility

## Notes and Other Information
- Requires database owner privileges to execute
- Handles both COLLPROVIDER_LIBC and other collation providers by checking different catalog attributes
- Validates that version changes are consistent (cannot change from NULL to non-NULL or vice versa)
- Uses InplaceUpdateTupleLock to ensure atomic updates to the catalog
- Part of PostgreSQL's collation version tracking mechanism introduced to detect potential corruption after collation library updates
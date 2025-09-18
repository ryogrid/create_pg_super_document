# RenameDatabase

## Location
src/backend/commands/dbcommands.c: 1863 - 1963

## Overview
RenameDatabase renames an existing PostgreSQL database by updating the database name in the system catalog while ensuring proper locking and validation checks.

## Definition


## Detailed Description
RenameDatabase performs a complete database rename operation with comprehensive safety checks. The function acquires an exclusive lock on the target database to prevent concurrent access, validates ownership and privileges, ensures the new name doesn't conflict with existing databases, and updates the database name in the pg_database system catalog. The operation includes special handling to prevent renaming the currently connected database and ensures no other active sessions are using the database during the rename process.

## Parameters / Member Variables
- : The current name of the database to be renamed
- : The desired new name for the database

## Dependencies
- Functions called/Symbols referenced:
  - get_db_info: Retrieves database information and acquires locks
  - object_ownercheck: Verifies database ownership permissions
  - have_createdb_privilege: Checks if user has database creation privileges
  - get_database_oid: Looks up database OID by name
  - CountOtherDBBackends: Counts active connections to the database
  - SearchSysCacheLockedCopy1: Retrieves and locks database catalog tuple
  - namestrcpy: Copies the new name into the database tuple
  - CatalogTupleUpdate: Updates the database catalog entry
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
- Called from (representative examples):
  - ExecRenameStmt: Statement execution handler for RENAME operations

## Notes and Other Information
- Requires AccessExclusiveLock on the database to prevent concurrent operations
- Cannot rename the currently connected database (MyDatabaseId check)
- Validates that no other sessions are actively using the database
- Includes regression testing name validation when built with appropriate flags
- Returns ObjectAddress pointing to the renamed database for dependency tracking
- Maintains lock until transaction commit to ensure consistency
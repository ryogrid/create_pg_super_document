# AlterDatabaseSet

## Location
src/backend/commands/dbcommands.c: 2598 - 2623

## Overview
AlterDatabaseSet implements the ALTER DATABASE name SET configuration_parameter TO value command, which sets database-specific configuration parameters that apply to all sessions connecting to that database.

## Definition


## Detailed Description
This function handles the SQL command `ALTER DATABASE name SET parameter TO value` which allows setting database-specific configuration parameters. The function:

1. Resolves the database name to its OID using get_database_oid()
2. Acquires a shared dependency lock on the database to ensure it remains valid during the operation
3. Verifies that the current user owns the database (required for setting database parameters)
4. Delegates the actual parameter setting to AlterSetting() with the database OID
5. Releases the shared lock and returns the database OID

The function serves as a wrapper that handles permission checking and locking for database-specific configuration changes, with the core parameter management handled by the AlterSetting() function.

## Parameters / Member Variables
- `stmt`: Pointer to AlterDatabaseSetStmt containing the database name and parameter setting information

## Dependencies
- Functions called/Symbols referenced:
  - get_database_oid
  - shdepLockAndCheckObject
  - object_ownercheck
  - aclcheck_error
  - AlterSetting
  - UnlockSharedObject
- Called from (representative examples):
  - standard_ProcessUtility

## Notes and Other Information
- Requires database ownership privileges to execute
- Uses shared dependency locking to prevent the database from being dropped during the operation
- The actual parameter setting logic is delegated to AlterSetting() which handles the catalog updates
- Database-specific settings override global settings for sessions connecting to that database
- Part of PostgreSQL's configuration management system that allows per-database parameter customization
# RenameTableSpace

## Location
[src/backend/commands/tablespace.c:930-1014](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablespace.c#L930-L1014)

## Overview
Renames an existing tablespace by updating its name in the pg_tablespace system catalog, performing ownership and name validation checks.

## Definition
ObjectAddress RenameTableSpace(const char *oldname, const char *newname)

## Detailed Description
This function handles the complete process of renaming a tablespace in PostgreSQL. It performs several validation steps: verifying the old tablespace exists, checking the user has ownership privileges, validating the new name follows naming conventions (not reserved pg_ prefixes), and ensuring the new name doesn't conflict with existing tablespaces. The function operates on the pg_tablespace system catalog with row-exclusive locking to ensure consistency. After validation, it updates the tablespace name in the catalog and triggers post-alter hooks for dependency tracking. The function includes special regression testing checks when compiled with appropriate flags.

## Parameters / Member Variables
- oldname: The current name of the tablespace to be renamed
- newname: The desired new name for the tablespace

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md): Opens the pg_tablespace relation with specified lock
  - [ScanKeyInit](../S/ScanKeyInit.md): Initializes scan key for catalog searches
  - [table_beginscan_catalog](../t/table_beginscan_catalog.md): Begins catalog table scan
  - [heap_getnext](../h/heap_getnext.md): Retrieves next tuple from heap scan  
  - [heap_copytuple](../h/heap_copytuple.md): Creates a copy of heap tuple for modification
  - [table_endscan](../t/table_endscan.md): Ends table scan
  - [object_ownercheck](../o/object_ownercheck.md): Verifies user ownership of object
  - [aclcheck_error](../a/aclcheck_error.md): Reports access control errors
  - [IsReservedName](../I/IsReservedName.md): Checks if name uses reserved pg_ prefix
  - [namestrcpy](../n/namestrcpy.md): Copies string to Name data type
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates tuple in system catalog
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
  - ObjectAddressSet: Sets object address for return value

- Called from (representative examples):
  - [ExecRenameStmt](../E/ExecRenameStmt.md): General rename statement execution handler

## Notes and Other Information
- Requires RowExclusiveLock on pg_tablespace to prevent concurrent modifications
- Validates ownership using object_ownercheck() before allowing rename
- Prevents use of pg_ prefix in new names unless allowSystemTableMods is enabled
- Includes regression testing name convention checks when ENFORCE_REGRESSION_TEST_NAME_RESTRICTIONS is defined
- Returns ObjectAddress for use in dependency tracking and event processing
- Part of PostgreSQL's DDL command framework for tablespace management
- Integrates with the object dependency system through post-alter hooks
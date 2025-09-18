# ATExecSetStorage

## Location
src/backend/commands/tablecmds.c: 8887 - 8949

## Overview
ATExecSetStorage implements the ALTER TABLE ALTER COLUMN SET STORAGE command, modifying the storage strategy for a specific column in both the table and its associated indexes.

## Definition


## Detailed Description
This function modifies the storage strategy (PLAIN, EXTERNAL, EXTENDED, or MAIN) for a table column by updating the pg_attribute system catalog. The function validates the column existence, ensures it's not a system column, updates the storage setting in the catalog, and propagates the change to any associated indexes. It also triggers post-alter hooks to notify other subsystems of the change.

The storage strategy determines how PostgreSQL stores variable-length data types, affecting compression and out-of-line storage behavior for TOAST-able columns.

## Parameters / Member Variables
- `rel`: The relation (table) being modified
- `colName`: The name of the column whose storage is being changed
- `newValue`: A Node containing the new storage strategy value (PLAIN, EXTERNAL, EXTENDED, or MAIN)
- `lockmode`: The lock mode to use when accessing related indexes

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCacheCopyAttName
  - GetAttributeStorage
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - SetIndexStorageProperties
  - heap_freetuple
  - ObjectAddressSubSet
- Called from (representative examples):
  - ATExecCmd
  - child_dependency_type

## Notes and Other Information
- Located in src/backend/commands/tablecmds.c:8887-8949
- Returns an ObjectAddress pointing to the modified column
- Validates that the column is not a system column (attnum > 0)
- Automatically propagates storage changes to simple index columns
- Uses RowExclusiveLock on the pg_attribute catalog during the update
- The function is static, indicating it's only used within the tablecmds.c module
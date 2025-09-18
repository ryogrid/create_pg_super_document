# ATExecSetCompression

## Location
src/backend/commands/tablecmds.c: 17008 - 17087

## Overview
ATExecSetCompression handles the ALTER TABLE ALTER COLUMN SET COMPRESSION command by updating the compression method for a specific table column in the pg_attribute catalog and propagating the change to related indexes.

## Definition


## Detailed Description
This function implements column-level compression setting for ALTER TABLE operations. It validates that the specified column exists and is not a system column, checks that the column type supports compression, converts the compression method name to its internal code, updates the pg_attribute catalog, and applies the compression setting to any simple index columns that reference this table column.

The function performs these key operations:
1. Validates the column exists and is user-defined (not a system column)
2. Validates the compression method is compatible with the column's data type
3. Updates the pg_attribute catalog with the new compression method code
4. Propagates the compression setting to related index columns
5. Increments the command counter to make changes visible
6. Returns an ObjectAddress pointing to the modified column

## Parameters / Member Variables
- : The Relation structure representing the table being altered
- : The name of the column whose compression method is being set
- : A Node containing the string value of the compression method name
- : The lock mode to use when accessing related objects

## Dependencies
- Functions called/Symbols referenced:
  - strVal
  - table_open
  - SearchSysCacheCopyAttName
  - GetAttributeCompression
  - CatalogTupleUpdate
  - InvokeObjectPostAlterHook
  - SetIndexStorageProperties
  - heap_freetuple
  - table_close
  - CommandCounterIncrement
  - ObjectAddressSubSet

- Called from (representative examples):
  - ATExecCmd (main ALTER TABLE command dispatcher)

## Notes and Other Information
- Returns an ObjectAddress pointing to the modified column for dependency tracking
- System columns (attnum <= 0) cannot have their compression method altered
- The compression method name is validated against the column's data type via GetAttributeCompression
- Changes are automatically propagated to simple index columns that reference this table column
- Uses RowExclusiveLock when accessing the pg_attribute catalog
- CommandCounterIncrement ensures the changes are visible to subsequent operations in the same transaction
- The function matches the behavior of index.c ConstructTupleDescriptor() when handling index columns
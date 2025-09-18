# ATExecDropNotNull

## Location
src/backend/commands/tablecmds.c: 7556 - 7690

## Overview
Executes the ALTER TABLE ALTER COLUMN DROP NOT NULL command, performing validation checks and updating the system catalog to remove the NOT NULL constraint from a column.

## Definition
```c
static ObjectAddress ATExecDropNotNull(Relation rel, const char *colName, LOCKMODE lockmode)
```

## Detailed Description
This function implements the execution phase of dropping a NOT NULL constraint from a table column. It performs comprehensive validation to ensure the operation is safe and maintains database integrity. The function checks that the column exists, is not a system column, is not an identity column, is not part of a primary key or replica identity index, and for partitioned tables, ensures the parent table doesn't enforce NOT NULL on the same column.

The function returns the ObjectAddress of the modified column if the constraint was actually removed, or InvalidObjectAddress if the column was already nullable. It handles the catalog update by modifying the attnotnull field in pg_attribute and triggers post-alter hooks for proper event handling.

## Parameters / Member Variables
- `rel`: The relation (table) being altered
- `colName`: The name of the column to remove NOT NULL constraint from
- `lockmode`: The lock mode to use for the operation

## Dependencies
- Functions called/Symbols referenced:
  - table_open (opens system catalog tables)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md) (searches for attribute by name)
  - HeapTupleIsValid (validates heap tuple)
  - RelationGetRelid (gets relation OID)
  - RelationGetRelationName (gets relation name)
  - GETSTRUCT (extracts structure from heap tuple)
  - ereport/errmsg (error reporting)
  - [RelationGetIndexList](../R/RelationGetIndexList.md) (gets list of indexes on relation)
  - foreach_oid (iterates over OID list)
  - [SearchSysCache1](../S/SearchSysCache1.md) (searches system cache)
  - elog (error logging)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (releases system cache entries)
  - [list_free](../l/list_free.md) (frees list memory)
  - [get_partition_parent](../g/get_partition_parent.md) (gets parent of partition)
  - [get_attnum](../g/get_attnum.md) (gets attribute number by name)
  - TupleDescAttr (accesses tuple descriptor attributes)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (updates system catalog)
  - ObjectAddressSubSet (sets object address components)
  - InvokeObjectPostAlterHook (triggers post-alter hooks)
  - table_close (closes system catalog tables)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command execution dispatcher)

## Notes and Other Information
- The function is static, meaning it's only used within tablecmds.c
- Returns ObjectAddress of modified column or InvalidObjectAddress if no change made
- Performs extensive validation including primary key, replica identity, and partition hierarchy checks
- Uses RowExclusiveLock on pg_attribute for catalog updates
- Prevents dropping NOT NULL from system columns (attnum <= 0)
- Prevents dropping NOT NULL from identity columns
- For partitions, ensures parent table doesn't enforce NOT NULL on the same column
- Triggers InvokeObjectPostAlterHook for proper event notification
- Part of PostgreSQL's ALTER TABLE infrastructure (execution phase)
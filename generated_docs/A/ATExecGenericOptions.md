# ATExecGenericOptions

## Location
src/backend/commands/tablecmds.c: 16927 - 17007

## Overview
ATExecGenericOptions modifies the options of a foreign table by updating the pg_foreign_table catalog entry with new or modified options provided via ALTER FOREIGN TABLE OPTIONS statement.

## Definition


## Detailed Description
This function handles the ALTER FOREIGN TABLE OPTIONS command by updating the options stored in the pg_foreign_table system catalog. It validates the new options against the foreign data wrapper's validator function, updates the catalog tuple with the transformed options, and invalidates relevant caches to ensure all sessions refresh their cached plans that depend on the old options.

The function performs several key operations:
1. Retrieves the existing foreign table entry from pg_foreign_table
2. Gets the foreign server and foreign data wrapper information  
3. Extracts current options from the catalog
4. Transforms and validates the new options using the FDW's validator
5. Updates the catalog tuple with the new options
6. Invalidates the relation cache and invokes post-alter hooks

## Parameters / Member Variables
- : The Relation structure representing the foreign table being altered
- : A List of DefElem structures containing the new options to set or modify

## Dependencies
- Functions called/Symbols referenced:
  - table_open
  - SearchSysCacheCopy1
  - [GetForeignServer](../G/GetForeignServer.md)
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - InvokeObjectPostAlterHook
  - table_close
  - [heap_freetuple](../h/heap_freetuple.md)

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- This function is specifically for foreign tables only - it will error if called on a non-foreign table
- The function returns early if the options list is empty (NIL)
- Uses RowExclusiveLock when accessing the pg_foreign_table catalog
- The options validation is performed by the foreign data wrapper's validator function
- Cache invalidation ensures that all sessions see the updated options immediately
- Post-alter hooks are invoked to allow extensions to react to the option changes
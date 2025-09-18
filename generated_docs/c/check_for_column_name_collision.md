# check_for_column_name_collision

## Location
src/backend/commands/tablecmds.c: 7438 - 7490

## Overview
Checks if a new or renamed column name would collide with an existing column name in a relation, handling both error and if-not-exists scenarios.

## Definition


## Detailed Description
This function validates whether adding or renaming a column would create a name collision with an existing column in the specified relation. It performs a lookup in the pg_attribute system catalog to check for existing columns with the given name. The function handles different scenarios based on the if_not_exists parameter and whether the collision is with a system column or user column.

The function deliberately ignores dropped columns (attisdropped) during the check, as attempting to add a column with a dropped column's name would fail anyway. It provides different error messages for system column conflicts versus user column conflicts to help users understand the nature of the collision.

## Parameters / Member Variables
- : The relation (table) to check for column name collisions
- : The proposed column name to check for conflicts
- : If true, emit a notice and return false on collision; if false, emit an error on collision

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md) (searches pg_attribute by relation OID and column name)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - [PointerGetDatum](../P/PointerGetDatum.md)
  - HeapTupleIsValid
  - RelationGetRelid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - ereport (for error and notice reporting)
  - RelationGetRelationName
- Called from (representative examples):
  - [ATExecAddColumn](../A/ATExecAddColumn.md) (when adding new columns)
  - [renameatt_internal](../r/renameatt_internal.md) (when renaming existing columns)

## Notes and Other Information
- Returns true if no collision exists or collision is handled gracefully
- Returns false only when if_not_exists is true and a collision occurs
- System columns (attnum <= 0) always trigger an error regardless of if_not_exists flag
- The function is static, meaning it's only used within tablecmds.c
- Deliberately does not consider dropped columns to avoid confusion with reusing dropped column names
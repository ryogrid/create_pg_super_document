# ATExecSetStatistics

## Location
src/backend/commands/tablecmds.c: 8610 - 8744

## Overview
ATExecSetStatistics executes the SET STATISTICS command for ALTER TABLE operations, modifying the statistics target for a specified column to control the level of statistics collection during ANALYZE operations.

## Definition
static ObjectAddress ATExecSetStatistics(Relation rel, const char *colName, int16 colNum, Node *newValue, LOCKMODE lockmode)

## Detailed Description
This function implements the execution of ALTER TABLE ALTER COLUMN SET STATISTICS commands, which control how much statistical information PostgreSQL collects about a column during ANALYZE operations. The function validates the target value, ensures it falls within acceptable bounds, and updates the pg_attribute catalog. It supports both named column references and numeric column references (for indexes only). The statistics target affects query planning by determining the sample size and histogram detail for the column.

The function handles special validation for index columns, ensuring that statistics can only be set on expression columns and not on regular indexed columns, since statistics on regular columns should be set on the underlying table column instead.

## Parameters / Member Variables
- : The relation (table or index) containing the column to be modified
- : The name of the column (NULL if using numeric reference)
- : The column number (used for index columns when colName is NULL)
- : Node containing the new statistics target value (-1 or NULL for default)
- : The lock mode to use for accessing related catalog tables

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_INDEX
  - RELKIND_PARTITIONED_INDEX
  - intVal
  - MAX_STATISTICS_TARGET
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md)
  - [SearchSysCacheAttNum](../S/SearchSysCacheAttNum.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSubSet
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- Statistics targets are limited to MAX_STATISTICS_TARGET, with automatic adjustment and warning for higher values
- Negative values (except -1 for default) are rejected with an error
- Column references by number are only allowed for indexes, not tables
- System columns cannot have their statistics targets modified
- For indexes, statistics can only be set on expression columns, not on regular indexed columns
- The function supports both setting explicit targets and resetting to default (NULL)
# ATExecSetOptions

## Location
[src/backend/commands/tablecmds.c:8745-8823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8745-L8823)

## Overview
ATExecSetOptions executes the SET OPTIONS or RESET OPTIONS command for ALTER TABLE operations, modifying or resetting the storage options for a specified column.

## Definition
static ObjectAddress ATExecSetOptions(Relation rel, const char *colName, Node *options, bool isReset, LOCKMODE lockmode)

## Detailed Description
This function implements the execution of ALTER TABLE ALTER COLUMN SET/RESET OPTIONS commands, which allow users to modify storage-related options for individual columns. The function retrieves the current column options, transforms them by applying the new options or resetting existing ones, validates the resulting options using attribute_reloptions, and updates the pg_attribute catalog. The options are stored as a text array in the attoptions field and can control various column-specific storage parameters.

The function ensures that only valid attribute options are applied and maintains consistency by validating all options before committing the changes to the catalog.

## Parameters / Member Variables
- : The relation (table) containing the column to be modified
- : The name of the column whose options are being modified
- : Node containing the list of options to set (parsed from SQL)
- : Boolean flag indicating whether to reset options instead of setting new ones
- : The lock mode to use for accessing related catalog tables

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [transformRelOptions](../t/transformRelOptions.md)
  - [attribute_reloptions](../a/attribute_reloptions.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSubSet
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- System columns cannot have their options modified
- The function validates new options before applying them to prevent invalid configurations
- Options are stored as a text array in the attoptions field of pg_attribute
- The isReset flag determines whether to clear existing options or merge with new ones
- Uses transformRelOptions to handle the complex logic of option merging and validation
- Returns the ObjectAddress of the modified column for dependency tracking
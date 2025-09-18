# ATExecSetRelOptions

## Location
[src/backend/commands/tablecmds.c:15049-15252](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L15049-L15252)

## Overview
ATExecSetRelOptions executes the ALTER TABLE SET/RESET/REPLACE relation options commands, updating storage parameters and configuration options for database relations and their associated TOAST tables.

## Definition
```c
static void ATExecSetRelOptions(Relation rel, List *defList, AlterTableType operation, LOCKMODE lockmode)
```

## Detailed Description
This function implements the execution phase for ALTER TABLE commands that modify relation options (reloptions). It handles three types of operations: setting new options, resetting options to defaults, and completely replacing the options list. The function validates the new options based on the relation kind (table, view, index, etc.), updates the pg_class system catalog, and also processes any associated TOAST table.

The function follows a comprehensive workflow: it retrieves existing options, transforms the new option list, validates the options against the relation type, updates the system catalog, and handles TOAST table options separately. Special validation is performed for views with CHECK OPTION to ensure they are auto-updatable.

## Parameters / Member Variables
- `rel`: The relation being modified  
- `defList`: List of DefElem structures containing the new option definitions
- `operation`: Type of operation (AT_SetRelOptions, AT_ResetRelOptions, or AT_ReplaceRelOptions)
- `lockmode`: Lock mode for accessing related objects

## Dependencies
- Functions called/Symbols referenced:
  - [transformRelOptions](../t/transformRelOptions.md): Processes and validates the option list
  - [heap_reloptions](../h/heap_reloptions.md): Validates options for heap tables
  - [partitioned_table_reloptions](../p/partitioned_table_reloptions.md): Validates options for partitioned tables  
  - [view_reloptions](../v/view_reloptions.md): Validates options for views
  - [index_reloptions](../i/index_reloptions.md): Validates options for indexes
  - [get_view_query](../g/get_view_query.md): Retrieves the query definition for views
  - [view_query_is_auto_updatable](../v/view_query_is_auto_updatable.md): Checks if view supports CHECK OPTION
  - [SearchSysCacheLocked1](../S/SearchSysCacheLocked1.md): Looks up relation tuple in system cache
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the pg_class system catalog
  - [heap_modify_tuple](../h/heap_modify_tuple.md): Creates modified version of heap tuple
  - InvokeObjectPostAlterHook: Triggers post-alter hooks
  - [errdetail_relkind_not_supported](../e/errdetail_relkind_not_supported.md): Generates error details for unsupported relation kinds

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md): Main ALTER TABLE command execution dispatcher

## Notes and Other Information
- Supports different relation kinds: regular tables, partitioned tables, materialized views, views, indexes, and TOAST tables
- Automatically handles TOAST table option updates when modifying the main table
- Performs special validation for views with CHECK OPTION to ensure auto-updatability
- Uses relation-specific validation functions based on the relation kind
- Updates are propagated to relation caches during post-commit cache invalidation
- Handles three operation types: setting new options, resetting to defaults, and complete replacement
- Maintains transactional safety by using appropriate locking and system catalog updates
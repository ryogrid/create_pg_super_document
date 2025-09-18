# ATExecDropOf

## Location
src/backend/commands/tablecmds.c: 16628 - 16671

## Overview
Detaches a typed table from its originating type by clearing the relationship metadata and removing the associated dependency.

## Definition


## Detailed Description
ATExecDropOf implements the  SQL command functionality. When a table is created with , it establishes a typed table relationship where the table's structure is tied to a composite type. This function reverses that relationship by:

1. Validating that the relation is actually a typed table (has a valid reloftype)
2. Removing the dependency relationship between the table and its type
3. Clearing the  field in the  catalog entry
4. Triggering post-alter hooks for proper event notification

The function assumes that ownership of the table provides sufficient rights to perform this operation, without requiring additional type ownership checks or locks on the type itself.

## Parameters / Member Variables
- : The relation (table) to detach from its type
- : The lock mode to use (parameter present but not actively used in the function body)

## Dependencies
- Functions called/Symbols referenced:
  - [drop_parent_dependency](../d/drop_parent_dependency.md): Removes the dependency relationship between table and type
  - SearchSysCacheCopy1: Retrieves a copy of the relation's catalog tuple
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md): Updates the modified tuple in the pg_class catalog
  - InvokeObjectPostAlterHook: Triggers post-alter event hooks
  - [heap_freetuple](../h/heap_freetuple.md): Frees the heap tuple memory
  - DEPENDENCY_NORMAL: Constant indicating normal dependency type

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md): Main ALTER TABLE command execution dispatcher

## Notes and Other Information
- The function performs validation to ensure the table is actually typed before attempting detachment
- No ownership check is performed on the type itself - table ownership is considered sufficient
- The function uses RowExclusiveLock when modifying the pg_class catalog
- Post-alter hooks are invoked to maintain consistency with PostgreSQL's event system
- Error handling includes both user-facing errors (wrong object type) and internal errors (cache lookup failures)
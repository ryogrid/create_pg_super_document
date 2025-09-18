# ATExecDropColumn

## Location
src/backend/commands/tablecmds.c: 8978 - 9178

## Overview
ATExecDropColumn implements the execution phase of ALTER TABLE DROP COLUMN, handling the complex logic of dropping columns from tables while managing inheritance hierarchies, partition constraints, and cascading effects.

## Definition


## Detailed Description
This function orchestrates the dropping of a column from a relation, handling complex scenarios including inheritance hierarchies, partitioned tables, and system constraints. It performs extensive validation (system columns, inherited columns, partition key usage), manages recursive descent through child relations, and coordinates the final deletion through PostgreSQL's dependency system. The function uses a two-phase approach: collecting objects to delete during recursion, then performing all deletions atomically at the top level.

The function is recursive and handles different behaviors for inheritance children based on whether they have local definitions or are purely inherited.

## Parameters / Member Variables
- `wqueue`: Work queue for storing additional ALTER TABLE commands
- `rel`: The relation (table) from which to drop the column
- `colName`: Name of the column to be dropped
- `behavior`: Drop behavior (CASCADE or RESTRICT) for handling dependencies
- `recurse`: Whether to recurse through inheritance hierarchy
- `recursing`: Flag indicating if this is a recursive call
- `missing_ok`: Whether to emit notice instead of error if column doesn't exist
- `lockmode`: Lock mode for accessing child relations
- `addrs`: Collection of object addresses to delete (used in recursion)

## Dependencies
- Functions called/Symbols referenced:
  - [ATSimplePermissions](ATSimplePermissions.md)
  - check_stack_depth
  - [new_object_addresses](../n/new_object_addresses.md)
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
  - [has_partition_attrs](../h/has_partition_attrs.md)
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - CommandCounterIncrement
  - [add_exact_object_address](../a/add_exact_object_address.md)
  - [performMultipleDeletions](../p/performMultipleDeletions.md)
  - [free_object_addresses](../f/free_object_addresses.md)
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - [ATExecDropColumn](ATExecDropColumn.md) (recursive)
  - child_dependency_type

## Notes and Other Information
- Located in src/backend/commands/tablecmds.c:8978-9178
- Returns ObjectAddress of the dropped column
- Validates against dropping system columns (attnum <= 0)
- Prevents dropping inherited columns unless recursing from parent
- Prevents dropping partition key columns to avoid cascaded table deletion
- Handles partitioned tables by requiring explicit recursion to child partitions
- Uses inheritance count management for child relations (decrement vs. delete)
- Implements stack overflow protection due to recursive nature
- Atomic deletion of all collected objects at top-level completion
- Sets attislocal=true for child columns when parent column dropped without recursion
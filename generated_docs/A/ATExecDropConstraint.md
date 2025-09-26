# ATExecDropConstraint

## Location
[src/backend/commands/tablecmds.c:12556-12806](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L12556-L12806)

## Overview
Executes ALTER TABLE DROP CONSTRAINT commands, handling constraint deletion with inheritance recursion and proper dependency management.

## Definition

```c
static void
ATExecDropConstraint(Relation rel, const char *constrName,
					 DropBehavior behavior,
					 bool recurse, bool recursing,
					 bool missing_ok, LOCKMODE lockmode)
```
## Detailed Description
This function implements constraint deletion for ALTER TABLE operations. Unlike normal ALTER TABLE recursion, it uses a custom recursion mechanism to properly handle inherited constraints. The function searches for the target constraint in pg_constraint, validates permissions, handles foreign key locking requirements, performs the actual deletion via the dependency system, and recursively processes child tables. It properly manages inheritance counts for CHECK constraints and handles both CASCADE and RESTRICT behaviors. For partitioned tables, it enforces that constraints cannot be dropped from only the parent when partitions exist.

## Parameters / Member Variables
- : The relation from which to drop the constraint
- : Name of the constraint to drop
- : CASCADE or RESTRICT behavior for dependency handling
- : Whether to recursively drop from child tables
- : True when called recursively on child tables
- : Whether to report error if constraint doesn't exist
- : Lock level to use when accessing child relations

## Dependencies
- Functions called/Symbols referenced:
  - [ATSimplePermissions](ATSimplePermissions.md) (permission checking)
  - [table_open](../t/table_open.md)/table_close (relation access)
  - [systable_beginscan](../s/systable_beginscan.md)/systable_endscan (catalog scanning)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md) (safety validation)
  - [performDeletion](../p/performDeletion.md) (dependency-based deletion)
  - [find_inheritance_children](../f/find_inheritance_children.md) (inheritance hierarchy)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md) (catalog updates)
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md) (visibility control)
  - [heap_copytuple](../h/heap_copytuple.md)/heap_freetuple (tuple management)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE executor)
  - [ATExecDropConstraint](ATExecDropConstraint.md) (recursive self-calls)

## Notes and Other Information
- Cannot use normal ALTER TABLE recursion due to special inheritance handling requirements
- Prevents dropping inherited constraints unless recursing from parent
- For foreign key constraints, locks referenced table to prevent concurrent modifications
- CHECK constraints are handled with inheritance count management
- Non-CHECK constraints on partitioned tables are handled via dependency mechanism
- Supports IF EXISTS semantics via missing_ok parameter
- Uses custom one-level-at-a-time recursion for proper constraint inheritance handling
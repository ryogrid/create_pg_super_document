# ATPrepDropColumn

## Location
[src/backend/commands/tablecmds.c:8950-8977](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8950-L8977)

## Overview
ATPrepDropColumn is a preparation function for ALTER TABLE DROP COLUMN that handles special cases and recursion setup before the actual column dropping operation is executed.

## Definition

```c
static void
ATPrepDropColumn(List **wqueue, Relation rel, bool recurse, bool recursing,
				 AlterTableCmd *cmd, LOCKMODE lockmode,
				 AlterTableUtilityContext *context)
```
## Detailed Description
This function performs preparatory work for dropping a column from a table. It validates that the operation is allowed (preventing drops from typed tables unless recursing), handles special recursion for composite types through ATTypedTableRecursion, and sets up the recursion flag for inheritance hierarchies. Unlike normal ALTER TABLE operations, DROP COLUMN cannot use standard recursion mechanisms because inheritance count decisions must be made at runtime.

The function serves as a gate-keeper and setup routine, ensuring that the subsequent ATExecDropColumn operation will have the proper context and permissions to proceed safely.

## Parameters / Member Variables
- `wqueue`: Work queue for storing ALTER TABLE subcommands to be processed
- `rel`: The relation (table) from which a column will be dropped
- `recurse`: Boolean flag indicating whether to recurse through inheritance hierarchy
- `recursing`: Boolean flag indicating if this call is part of an ongoing recursion
- `cmd`: The ALTER TABLE command structure containing drop column details
- `lockmode`: The lock mode to use for accessing related objects
- `context`: Utility context containing session and transaction information

## Dependencies
- Functions called/Symbols referenced:
  - [ATTypedTableRecursion](ATTypedTableRecursion.md)
  - [AlterTableCmd](AlterTableCmd.md) (struct)
  - [AlterTableUtilityContext](AlterTableUtilityContext.md) (struct)
  - RELKIND_COMPOSITE_TYPE (constant)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md)
  - child_dependency_type

## Notes and Other Information
- Located in src/backend/commands/tablecmds.c:8950-8977
- Static function used only within the tablecmds.c module
- Prevents dropping columns from typed tables (tables based on composite types) unless in recursion
- Special handling for composite types through ATTypedTableRecursion
- Cannot use normal ALTER TABLE recursion due to runtime inheritance count decisions
- Sets the recurse flag on the command structure for later processing phases
- Part of the two-phase ALTER TABLE processing (preparation and execution phases)
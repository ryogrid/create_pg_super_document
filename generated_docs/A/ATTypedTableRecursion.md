# ATTypedTableRecursion

## Location
[src/backend/commands/tablecmds.c:6693-6737](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6693-L6737)

## Overview
A specialized recursion function that propagates ALTER TYPE operations from composite types to their dependent typed tables, handling CASCADE/RESTRICT behavior and inheritance recursion.

## Definition

```c
static void
ATTypedTableRecursion(List **wqueue, Relation rel, AlterTableCmd *cmd,
					  LOCKMODE lockmode, AlterTableUtilityContext *context)
```
## Detailed Description
ATTypedTableRecursion handles the complex propagation of ALTER TYPE operations on composite types to all tables that are defined as being "OF" that type (typed tables). When a composite type is altered, all typed tables based on that type must be updated accordingly. The function uses find_typed_table_dependencies to locate all dependent typed tables, respecting the CASCADE/RESTRICT behavior specified in the command. For each dependent typed table found, it performs safety validation and adds the ALTER command to the work queue with recursion enabled, allowing the changes to further propagate to inheritance children of the typed tables if applicable.

## Parameters / Member Variables
- `**wqueue`: Double pointer to the work queue list where ALTER TABLE commands for dependent typed tables are added
- `rel`: The Relation structure representing the composite type being altered (must be RELKIND_COMPOSITE_TYPE)
- `*cmd`: The AlterTableCmd structure containing the ALTER TYPE command and its behavior settings
- `lockmode`: The lock mode to use when accessing dependent typed tables
- `*context`: The AlterTableUtilityContext providing additional context for the ALTER TABLE operation
## Dependencies
- Functions called/Symbols referenced:
  - [find_typed_table_dependencies](../f/find_typed_table_dependencies.md)
  - [relation_open](../r/relation_open.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - [ATPrepCmd](ATPrepCmd.md)
  - [relation_close](../r/relation_close.md)
  - RelationGetRelationName
- Called from (representative examples):
  - [ATPrepAddColumn](ATPrepAddColumn.md)
  - [ATPrepDropColumn](ATPrepDropColumn.md)
  - [ATPrepAlterColumnType](ATPrepAlterColumnType.md)

## Notes and Other Information
- Only operates on composite types (RELKIND_COMPOSITE_TYPE), enforced by an assertion
- Respects CASCADE/RESTRICT behavior through find_typed_table_dependencies function
- Enables recursion in the ATPrepCmd call, allowing changes to propagate to inheritance children of typed tables
- Performs safety validation on each dependent typed table before processing
- Essential for maintaining consistency between composite types and their dependent typed tables
- The function ensures that structural changes to composite types are properly reflected in all tables defined as being "OF" that type

## Simplified Source

```c
static void ATTypedTableRecursion(List **wqueue, Relation rel, AlterTableCmd *cmd,
                                 LOCKMODE lockmode, AlterTableUtilityContext *context) {
    List *children;

    // Ensure we're working with a composite type
    Assert(rel->rd_rel->relkind == RELKIND_COMPOSITE_TYPE);

    // Find all typed tables that depend on this composite type
    children = find_typed_table_dependencies(rel->rd_rel->reltype,
                                           RelationGetRelationName(rel),
                                           cmd->behavior);

    // Process each dependent typed table
    foreach(child, children) {
        Oid childrelid = lfirst_oid(child);
        Relation childrel;

        // Open the dependent typed table
        childrel = relation_open(childrelid, lockmode);

        // Validate that the table can be safely altered
        CheckAlterTableIsSafe(childrel);

        // Add the ALTER command to the work queue for this typed table
        // Enable recursion to propagate to inheritance children
        ATPrepCmd(wqueue, childrel, cmd, true, true, lockmode, context);

        relation_close(childrel, NoLock);
    }
}
```
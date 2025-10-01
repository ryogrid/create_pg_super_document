# ATSimpleRecursion

## Location
[src/backend/commands/tablecmds.c:6618-6662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L6618-L6662)

## Overview
A table inheritance recursion function that applies ALTER TABLE commands to all direct and indirect child tables in an inheritance hierarchy when recursion is requested.

## Definition

```c
static void
ATSimpleRecursion(List **wqueue, Relation rel,
				  AlterTableCmd *cmd, bool recurse, LOCKMODE lockmode,
				  AlterTableUtilityContext *context)
```
## Detailed Description
ATSimpleRecursion implements the standard recursion pattern for ALTER TABLE operations that need to be propagated down inheritance hierarchies. When recursion is enabled and the target relation has child tables, it uses find_all_inheritors to discover all relations in the inheritance tree, then processes each child by adding the ALTER command to the work queue via ATPrepCmd. The function ensures that each child table is visited exactly once, even if it inherits from the parent through multiple inheritance paths. It performs safety checks on each child relation before processing and maintains proper locking throughout the operation.

## Parameters / Member Variables
- : Double pointer to the work queue list where ALTER TABLE commands for child relations are added
- : The parent Relation structure that serves as the root of the inheritance hierarchy
- : The AlterTableCmd structure containing the specific ALTER TABLE command to be applied
- : Boolean flag indicating whether to recursively apply the command to child tables
- : The lock mode to be used when accessing child relations during the recursion
- : The AlterTableUtilityContext providing additional context for the ALTER TABLE operation

## Dependencies
- Functions called/Symbols referenced:
  - [find_all_inheritors](../f/find_all_inheritors.md)
  - [relation_open](../r/relation_open.md)
  - [CheckAlterTableIsSafe](../C/CheckAlterTableIsSafe.md)
  - [ATPrepCmd](ATPrepCmd.md)
  - [relation_close](../r/relation_close.md)
  - RelationGetRelid
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md) (for various ALTER TABLE command types)
  - [ATPrepSetNotNull](ATPrepSetNotNull.md)

## Notes and Other Information
- Only processes recursion when the recurse flag is true and the relation has child tables (relhassubclass)
- Uses find_all_inheritors to handle complex inheritance hierarchies automatically
- Skips the original relation itself when processing the inheritance list
- Relies on find_all_inheritors for proper locking, using NoLock for subsequent operations
- Each child relation undergoes safety validation via CheckAlterTableIsSafe before command preparation
- Represents the standard recursion pattern used by most ALTER TABLE operations that support inheritance

## Simplified Source

```c
static void
ATSimpleRecursion(List **wqueue, Relation rel,
                  AlterTableCmd *cmd, bool recurse, LOCKMODE lockmode,
                  AlterTableUtilityContext *context)
{
    // Only recurse if requested and relation has child tables
    if (recurse && rel->rd_rel->relhassubclass)
    {
        Oid relid = RelationGetRelid(rel);
        List *children;
        ListCell *child;

        // Find all tables in inheritance hierarchy
        children = find_all_inheritors(relid, lockmode, NULL);

        // Process each child table
        foreach(child, children)
        {
            Oid childrelid = lfirst_oid(child);
            Relation childrel;

            // Skip the original relation itself
            if (childrelid == relid)
                continue;

            // Open child relation and apply ALTER command
            childrel = relation_open(childrelid, NoLock);
            CheckAlterTableIsSafe(childrel);
            ATPrepCmd(wqueue, childrel, cmd, false, true, lockmode, context);
            relation_close(childrel, NoLock);
        }
    }
}
```
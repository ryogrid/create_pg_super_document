# ATPrepDropExpression

## Location
[src/backend/commands/tablecmds.c:8473-8518](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8473-L8518)

## Overview
ATPrepDropExpression prepares and validates the DROP EXPRESSION command for ALTER TABLE operations, ensuring that dropping a generated column expression is valid and properly handles inheritance scenarios.

## Definition

```c
static void
ATPrepDropExpression(Relation rel, AlterTableCmd *cmd, bool recurse, bool recursing, LOCKMODE lockmode)
```
## Detailed Description
This function performs preliminary validation for dropping a generated column expression as part of an ALTER TABLE command. It enforces PostgreSQL's inheritance rules by requiring that DROP EXPRESSION operations cascade to child tables when inheritance relationships exist. The function also prevents dropping generation expressions from inherited columns, as these expressions are considered part of the column definition and cannot be selectively removed from inherited columns.

The function implements two key safety checks: first, it rejects ONLY operations when child tables exist (requiring explicit recursion), and second, it prevents dropping expressions from columns that are inherited from parent tables.

## Parameters / Member Variables
- : The relation (table) being altered
- : The ALTER TABLE command structure containing the column name and operation details
- : Boolean flag indicating whether the operation should cascade to child tables
- : Boolean flag indicating whether this call is part of a recursive operation on child tables
- : The lock mode to use when accessing related tables

## Dependencies
- Functions called/Symbols referenced:
  - [AlterTableCmd](AlterTableCmd.md) (structure)
  - [find_inheritance_children](../f/find_inheritance_children.md)
  - [SearchSysCacheCopyAttName](../S/SearchSysCacheCopyAttName.md)
- Called from (representative examples):
  - [ATPrepCmd](ATPrepCmd.md)

## Notes and Other Information
- The function contains detailed comments explaining why ONLY operations are not implemented for DROP EXPRESSION
- Generated expressions are treated differently from DEFAULT values in inheritance scenarios
- The function ensures that inheritance relationships are properly maintained when dropping column expressions
- Error messages provide clear feedback about unsupported operations and missing columns

## Simplified Source

```c
static void
ATPrepDropExpression(Relation rel, AlterTableCmd *cmd, bool recurse, bool recursing, LOCKMODE lockmode)
{
    // Require recursion if child tables exist (ONLY not supported)
    if (!recurse && find_inheritance_children(RelationGetRelid(rel), lockmode))
        ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                       errmsg("ALTER TABLE / DROP EXPRESSION must be applied to child tables too")));

    // Cannot drop generation expression from inherited columns
    if (!recursing) {
        HeapTuple tuple = SearchSysCacheCopyAttName(RelationGetRelid(rel), cmd->name);

        if (!HeapTupleIsValid(tuple))
            ereport(ERROR, (errcode(ERRCODE_UNDEFINED_COLUMN),
                           errmsg("column \"%s\" of relation \"%s\" does not exist",
                                  cmd->name, RelationGetRelationName(rel))));

        Form_pg_attribute attTup = (Form_pg_attribute) GETSTRUCT(tuple);

        if (attTup->attinhcount > 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_TABLE_DEFINITION),
                           errmsg("cannot drop generation expression from inherited column")));
    }
}
```
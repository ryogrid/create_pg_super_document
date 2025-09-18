# ATExecCookedColumnDefault

## Location
[src/backend/commands/tablecmds.c:7994-8022](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L7994-L8022)

## Overview
ATExecCookedColumnDefault adds a pre-cooked (pre-processed) default expression to a column in a PostgreSQL relation, returning the address of the affected column.

## Definition
```c
static ObjectAddress ATExecCookedColumnDefault(Relation rel, AttrNumber attnum, Node *newDefault)
```

## Detailed Description
This function is part of PostgreSQL's ALTER TABLE command infrastructure and is responsible for applying a pre-processed default expression to a column. The term "cooked" indicates that the default expression has already been analyzed, parsed, and transformed into its final form, unlike "raw" expressions that still need processing.

The function performs a two-step operation: first removing any existing default for the specified column, then storing the new default expression. It uses RESTRICT mode when removing the old default for safety, though typically no dependencies are expected on column defaults. The function is designed to handle cases where defaults might exist due to LIKE clause usage combined with inheritance scenarios.

## Parameters / Member Variables
- `rel`: The relation (table) containing the column to modify
- `attnum`: The attribute number (column number) within the relation
- `newDefault`: The pre-processed default expression node to be stored

## Dependencies
- Functions called/Symbols referenced:
  - [RemoveAttrDefault](../R/RemoveAttrDefault.md)
  - [StoreAttrDefault](../S/StoreAttrDefault.md)
  - ObjectAddressSubSet
  - DROP_RESTRICT
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type

## Notes and Other Information
- This is a static function within tablecmds.c, indicating it's an internal implementation detail of the ALTER TABLE infrastructure
- The function assumes no validation checking is required since the default expression is already "cooked"
- Uses RESTRICT mode for safety when removing old defaults, even though dependencies are typically not expected
- Handles edge cases involving LIKE clauses with inheritance where defaults might unexpectedly exist
- Returns an ObjectAddress that can be used for dependency tracking and catalog operations
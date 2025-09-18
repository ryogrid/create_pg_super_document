# ATExecSetExpression

## Location
src/backend/commands/tablecmds.c: 8360 - 8472

## Overview
ATExecSetExpression implements the ALTER TABLE ALTER COLUMN SET EXPRESSION command, allowing modification of the expression used by a generated column in PostgreSQL relations.

## Definition
```c
static ObjectAddress ATExecSetExpression(AlteredTableInfo *tab, Relation rel, const char *colName, Node *newExpr, LOCKMODE lockmode)
```

## Detailed Description
This function changes the generation expression of an existing stored generated column. Generated columns are computed columns whose values are derived from other columns in the same row using an expression. This operation requires a complete table rewrite since the values of the generated column need to be recalculated based on the new expression.

The function performs a complex multi-step process:
1. Validates that the target column exists and is a stored generated column
2. Clears any missing values since table rewrite renders them pointless
3. Records dependencies that need to be rebuilt after the table rewrite
4. Removes the old generated expression and its dependencies from system catalogs
5. Stores the new expression in the catalogs
6. Prepares for table rewrite by setting up new column values and marking the need for rewrite
7. Removes statistics for the column since they will be invalid after the expression change

The operation is complex because changing a generated expression affects not only the column definition but also requires recalculating all existing values and potentially rebuilding dependent objects.

## Parameters / Member Variables
- `tab`: AlteredTableInfo structure containing information about the table alteration context
- `rel`: The relation containing the generated column to modify
- `colName`: Name of the generated column whose expression should be changed
- `newExpr`: The new expression node to be used for generating column values
- `lockmode`: Lock mode for the operation (currently unused in this function)

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md)
  - [RelationClearMissing](../R/RelationClearMissing.md)
  - CommandCounterIncrement
  - [RememberAllDependentForRebuilding](../R/RememberAllDependentForRebuilding.md)
  - [GetAttrDefaultOid](../G/GetAttrDefaultOid.md)
  - [deleteDependencyRecordsFor](../d/deleteDependencyRecordsFor.md)
  - [RemoveAttrDefault](../R/RemoveAttrDefault.md)
  - [AddRelationNewConstraints](AddRelationNewConstraints.md)
  - [build_column_default](../b/build_column_default.md)
  - [expression_planner](../e/expression_planner.md)
  - [RemoveStatistics](../R/RemoveStatistics.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSubSet
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)
  - child_dependency_type

## Notes and Other Information
- This is a static function within tablecmds.c, part of the ALTER TABLE infrastructure
- Only works with stored generated columns (ATTRIBUTE_GENERATED_STORED), not virtual generated columns
- Requires a complete table rewrite (AT_REWRITE_DEFAULT_VAL) to recalculate all generated values
- Automatically handles dependency management by recording dependent objects for rebuilding
- Clears missing values optimization since table rewrite makes them irrelevant
- Removes column statistics since they become invalid when the generation expression changes
- Uses multiple CommandCounterIncrement() calls to ensure proper visibility of catalog changes
- The new expression is processed through expression_planner() to optimize it for execution
- Creates a NewColumnValue structure to prepare for the table rewrite phase
- Does not support regular inheritance propagation - only works on the specific relation specified
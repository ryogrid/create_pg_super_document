# ATExecSetExpression

## Location
[src/backend/commands/tablecmds.c:8360-8472](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8360-L8472)

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
  - [CommandCounterIncrement](../C/CommandCounterIncrement.md)
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

## Simplified Source

```c
static ObjectAddress
ATExecSetExpression(AlteredTableInfo *tab, Relation rel, const char *colName,
                    Node *newExpr, LOCKMODE lockmode)
{
    HeapTuple tuple;
    Form_pg_attribute attTup;
    AttrNumber attnum;
    Oid attrdefoid;
    ObjectAddress address;
    Expr *defval;
    NewColumnValue *newval;
    RawColumnDefault *rawEnt;

    // Find and validate the target column
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, "column does not exist");

    attTup = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = attTup->attnum;

    // Validate column can be altered
    if (attnum <= 0)
        ereport(ERROR, "cannot alter system column");
    if (attTup->attgenerated != ATTRIBUTE_GENERATED_STORED)
        ereport(ERROR, "column is not a generated column");
    ReleaseSysCache(tuple);

    // Clear missing values since we're rewriting the table
    RelationClearMissing(rel);
    CommandCounterIncrement();

    // Record dependencies for rebuilding after rewrite
    RememberAllDependentForRebuilding(tab, AT_SetExpression, rel, attnum, colName);

    // Remove old expression and its dependencies
    attrdefoid = GetAttrDefaultOid(RelationGetRelid(rel), attnum);
    if (!OidIsValid(attrdefoid))
        elog(ERROR, "could not find attrdef tuple");
    deleteDependencyRecordsFor(AttrDefaultRelationId, attrdefoid, false);
    CommandCounterIncrement();

    // Remove the old generated expression
    RemoveAttrDefault(RelationGetRelid(rel), attnum, DROP_RESTRICT, false, false);

    // Store the new expression
    rawEnt = (RawColumnDefault *) palloc(sizeof(RawColumnDefault));
    rawEnt->attnum = attnum;
    rawEnt->raw_default = newExpr;
    rawEnt->missingMode = false;
    rawEnt->generated = ATTRIBUTE_GENERATED_STORED;

    AddRelationNewConstraints(rel, list_make1(rawEnt), NIL, false, true, false, NULL);
    CommandCounterIncrement();

    // Prepare for table rewrite - build new column value
    defval = (Expr *) build_column_default(rel, attnum);
    newval = (NewColumnValue *) palloc0(sizeof(NewColumnValue));
    newval->attnum = attnum;
    newval->expr = expression_planner(defval);
    newval->is_generated = true;

    // Mark table for rewrite and add new value
    tab->newvals = lappend(tab->newvals, newval);
    tab->rewrite |= AT_REWRITE_DEFAULT_VAL;

    // Remove old statistics since they're invalid now
    RemoveStatistics(RelationGetRelid(rel), attnum);

    // Invoke post-alter hooks and return address
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);
    return address;
}
```
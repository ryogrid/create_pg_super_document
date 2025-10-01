# ATExecSetStatistics

## Location
[src/backend/commands/tablecmds.c:8610-8744](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L8610-L8744)

## Overview
ATExecSetStatistics executes the SET STATISTICS command for ALTER TABLE operations, modifying the statistics target for a specified column to control the level of statistics collection during ANALYZE operations.

## Definition
static ObjectAddress ATExecSetStatistics(Relation rel, const char *colName, int16 colNum, Node *newValue, LOCKMODE lockmode)

## Detailed Description
This function implements the execution of ALTER TABLE ALTER COLUMN SET STATISTICS commands, which control how much statistical information PostgreSQL collects about a column during ANALYZE operations. The function validates the target value, ensures it falls within acceptable bounds, and updates the pg_attribute catalog. It supports both named column references and numeric column references (for indexes only). The statistics target affects query planning by determining the sample size and histogram detail for the column.

The function handles special validation for index columns, ensuring that statistics can only be set on expression columns and not on regular indexed columns, since statistics on regular columns should be set on the underlying table column instead.

## Parameters / Member Variables
- : The relation (table or index) containing the column to be modified
- : The name of the column (NULL if using numeric reference)
- : The column number (used for index columns when colName is NULL)
- : Node containing the new statistics target value (-1 or NULL for default)
- : The lock mode to use for accessing related catalog tables

## Dependencies
- Functions called/Symbols referenced:
  - RELKIND_INDEX
  - RELKIND_PARTITIONED_INDEX
  - intVal
  - MAX_STATISTICS_TARGET
  - [SearchSysCacheAttName](../S/SearchSysCacheAttName.md)
  - [SearchSysCacheAttNum](../S/SearchSysCacheAttNum.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - InvokeObjectPostAlterHook
  - ObjectAddressSubSet
  - [heap_freetuple](../h/heap_freetuple.md)
- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- Statistics targets are limited to MAX_STATISTICS_TARGET, with automatic adjustment and warning for higher values
- Negative values (except -1 for default) are rejected with an error
- Column references by number are only allowed for indexes, not tables
- System columns cannot have their statistics targets modified
- For indexes, statistics can only be set on expression columns, not on regular indexed columns
- The function supports both setting explicit targets and resetting to default (NULL)

## Simplified Source

```c
static ObjectAddress
ATExecSetStatistics(Relation rel, const char *colName, int16 colNum, Node *newValue, LOCKMODE lockmode)
{
    int newtarget = 0;
    bool newtarget_default;
    Relation attrelation;
    HeapTuple tuple, newtuple;
    Form_pg_attribute attrtuple;
    AttrNumber attnum;
    ObjectAddress address;
    Datum repl_val[Natts_pg_attribute];
    bool repl_null[Natts_pg_attribute];
    bool repl_repl[Natts_pg_attribute];

    // Only allow column numbers for indexes
    if (rel->rd_rel->relkind != RELKIND_INDEX &&
        rel->rd_rel->relkind != RELKIND_PARTITIONED_INDEX &&
        !colName)
        ereport(ERROR, "cannot refer to non-index column by number");

    // Parse the new target value (-1 means default)
    if (newValue && intVal(newValue) != -1)
    {
        newtarget = intVal(newValue);
        newtarget_default = false;
    }
    else
        newtarget_default = true;

    // Validate target range
    if (!newtarget_default)
    {
        if (newtarget < 0)
            ereport(ERROR, "statistics target is too low");
        else if (newtarget > MAX_STATISTICS_TARGET)
        {
            newtarget = MAX_STATISTICS_TARGET;
            ereport(WARNING, "lowering statistics target to maximum");
        }
    }

    // Open attribute relation and find column
    attrelation = table_open(AttributeRelationId, RowExclusiveLock);
    if (colName)
        tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    else
        tuple = SearchSysCacheAttNum(RelationGetRelid(rel), colNum);

    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, "column does not exist");

    attrtuple = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = attrtuple->attnum;

    if (attnum <= 0)
        ereport(ERROR, "cannot alter system column");

    // Special validation for index columns
    if (rel->rd_rel->relkind == RELKIND_INDEX ||
        rel->rd_rel->relkind == RELKIND_PARTITIONED_INDEX)
    {
        if (attnum > rel->rd_index->indnkeyatts)
            ereport(ERROR, "cannot alter statistics on included column");
        else if (rel->rd_index->indkey.values[attnum - 1] != 0)
            ereport(ERROR, "cannot alter statistics on non-expression column");
    }

    // Build new tuple with updated statistics target
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));
    if (!newtarget_default)
        repl_val[Anum_pg_attribute_attstattarget - 1] = newtarget;
    else
        repl_null[Anum_pg_attribute_attstattarget - 1] = true;
    repl_repl[Anum_pg_attribute_attstattarget - 1] = true;

    newtuple = heap_modify_tuple(tuple, RelationGetDescr(attrelation),
                                 repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(attrelation, &tuple->t_self, newtuple);

    // Cleanup and post-alter processing
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attrtuple->attnum);
    ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);

    heap_freetuple(newtuple);
    ReleaseSysCache(tuple);
    table_close(attrelation, RowExclusiveLock);

    return address;
}
```
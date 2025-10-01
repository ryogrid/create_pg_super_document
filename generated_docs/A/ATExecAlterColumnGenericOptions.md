# ATExecAlterColumnGenericOptions

## Location
[src/backend/commands/tablecmds.c:14359-14475](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L14359-L14475)

## Overview
ATExecAlterColumnGenericOptions implements the ALTER COLUMN ... OPTIONS command for foreign table columns, allowing modification of foreign data wrapper specific options for individual columns.

## Definition
```c
static ObjectAddress
ATExecAlterColumnGenericOptions(Relation rel,
                                const char *colName,
                                List *options,
                                LOCKMODE lockmode)
```

## Detailed Description
This function handles the execution of ALTER COLUMN ... OPTIONS commands on foreign table columns. It validates that the target relation is indeed a foreign table, retrieves the associated foreign data wrapper and its validator function, then updates the column's FDW-specific options (attfdwoptions) in the pg_attribute system catalog. The function performs comprehensive validation including checking for the existence of the foreign table and column, preventing modification of system columns, and ensuring proper option transformation through the FDW's validator.

The function operates by opening the necessary system catalogs (pg_foreign_table and pg_attribute), retrieving the current column options, transforming them using the FDW validator, and updating the catalog with the new options. It handles both setting new options and clearing options (when transformed options result in NULL).

## Parameters
- `rel`: The foreign table relation being modified
- `colName`: Name of the column whose options are being altered
- `options`: List of option specifications to apply
- `lockmode`: Lock mode for the operation (unused in current implementation)

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md), table_close
  - [SearchSysCache1](../S/SearchSysCache1.md), SearchSysCacheAttName, ReleaseSysCache
  - [GetForeignServer](../G/GetForeignServer.md), GetForeignDataWrapper
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md), transformGenericOptions
  - [heap_modify_tuple](../h/heap_modify_tuple.md), CatalogTupleUpdate, heap_freetuple
  - InvokeObjectPostAlterHook, ObjectAddressSubSet
  - Form_pg_foreign_table, Form_pg_attribute
- Called from:
  - [ATExecCmd](ATExecCmd.md)

## Notes and Other Information
- Returns InvalidObjectAddress if no options are provided
- Prevents modification of system columns (attnum <= 0)
- Uses the FDW validator to ensure option validity and transformation
- Updates the attfdwoptions field in pg_attribute
- Fires post-alter hooks for proper event handling
- Handles both addition/modification and removal of options
- Requires the relation to be a foreign table registered in pg_foreign_table

## Simplified Source

```c
static ObjectAddress
ATExecAlterColumnGenericOptions(Relation rel,
                               const char *colName,
                               List *options,
                               LOCKMODE lockmode)
{
    Relation ftrel, attrel;
    ForeignServer *server;
    ForeignDataWrapper *fdw;
    HeapTuple tuple, newtuple;
    Form_pg_foreign_table fttableform;
    Form_pg_attribute atttableform;
    AttrNumber attnum;
    ObjectAddress address;
    Datum datum;
    bool isnull;
    Datum repl_val[Natts_pg_attribute] = {0};
    bool repl_null[Natts_pg_attribute] = {false};
    bool repl_repl[Natts_pg_attribute] = {false};

    if (options == NIL)
        return InvalidObjectAddress;

    // Get FDW validator from foreign table definition
    ftrel = table_open(ForeignTableRelationId, AccessShareLock);
    tuple = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(rel->rd_id));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, "foreign table \"%s\" does not exist", RelationGetRelationName(rel));

    fttableform = (Form_pg_foreign_table) GETSTRUCT(tuple);
    server = GetForeignServer(fttableform->ftserver);
    fdw = GetForeignDataWrapper(server->fdwid);
    table_close(ftrel, AccessShareLock);
    ReleaseSysCache(tuple);

    // Find the target column
    attrel = table_open(AttributeRelationId, RowExclusiveLock);
    tuple = SearchSysCacheAttName(RelationGetRelid(rel), colName);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, "column \"%s\" of relation \"%s\" does not exist",
                colName, RelationGetRelationName(rel));

    // Prevent modification of system columns
    atttableform = (Form_pg_attribute) GETSTRUCT(tuple);
    attnum = atttableform->attnum;
    if (attnum <= 0)
        ereport(ERROR, "cannot alter system column \"%s\"", colName);

    // Get current options and transform them using FDW validator
    datum = SysCacheGetAttr(ATTNAME, tuple, Anum_pg_attribute_attfdwoptions, &isnull);
    if (isnull)
        datum = PointerGetDatum(NULL);

    datum = transformGenericOptions(AttributeRelationId, datum, options, fdw->fdwvalidator);

    // Prepare update values
    if (PointerIsValid(DatumGetPointer(datum)))
        repl_val[Anum_pg_attribute_attfdwoptions - 1] = datum;
    else
        repl_null[Anum_pg_attribute_attfdwoptions - 1] = true;
    repl_repl[Anum_pg_attribute_attfdwoptions - 1] = true;

    // Update the catalog
    newtuple = heap_modify_tuple(tuple, RelationGetDescr(attrel),
                                repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(attrel, &newtuple->t_self, newtuple);

    // Fire post-alter hook and cleanup
    InvokeObjectPostAlterHook(RelationRelationId, RelationGetRelid(rel), attnum);
    ObjectAddressSubSet(address, RelationRelationId, RelationGetRelid(rel), attnum);

    ReleaseSysCache(tuple);
    table_close(attrel, RowExclusiveLock);
    heap_freetuple(newtuple);

    return address;
}
```
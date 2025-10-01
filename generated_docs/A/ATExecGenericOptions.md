# ATExecGenericOptions

## Location
[src/backend/commands/tablecmds.c:16927-17007](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/commands/tablecmds.c#L16927-L17007)

## Overview
ATExecGenericOptions modifies the options of a foreign table by updating the pg_foreign_table catalog entry with new or modified options provided via ALTER FOREIGN TABLE OPTIONS statement.

## Definition

```c
static void
ATExecGenericOptions(Relation rel, List *options)
```
## Detailed Description
This function handles the ALTER FOREIGN TABLE OPTIONS command by updating the options stored in the pg_foreign_table system catalog. It validates the new options against the foreign data wrapper's validator function, updates the catalog tuple with the transformed options, and invalidates relevant caches to ensure all sessions refresh their cached plans that depend on the old options.

The function performs several key operations:
1. Retrieves the existing foreign table entry from pg_foreign_table
2. Gets the foreign server and foreign data wrapper information  
3. Extracts current options from the catalog
4. Transforms and validates the new options using the FDW's validator
5. Updates the catalog tuple with the new options
6. Invalidates the relation cache and invokes post-alter hooks

## Parameters / Member Variables
- : The Relation structure representing the foreign table being altered
- : A List of DefElem structures containing the new options to set or modify

## Dependencies
- Functions called/Symbols referenced:
  - [table_open](../t/table_open.md)
  - SearchSysCacheCopy1
  - [GetForeignServer](../G/GetForeignServer.md)
  - [GetForeignDataWrapper](../G/GetForeignDataWrapper.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)
  - [transformGenericOptions](../t/transformGenericOptions.md)
  - [heap_modify_tuple](../h/heap_modify_tuple.md)
  - [CatalogTupleUpdate](../C/CatalogTupleUpdate.md)
  - [CacheInvalidateRelcache](../C/CacheInvalidateRelcache.md)
  - InvokeObjectPostAlterHook
  - [table_close](../t/table_close.md)
  - [heap_freetuple](../h/heap_freetuple.md)

- Called from (representative examples):
  - [ATExecCmd](ATExecCmd.md) (main ALTER TABLE command dispatcher)

## Notes and Other Information
- This function is specifically for foreign tables only - it will error if called on a non-foreign table
- The function returns early if the options list is empty (NIL)
- Uses RowExclusiveLock when accessing the pg_foreign_table catalog
- The options validation is performed by the foreign data wrapper's validator function
- Cache invalidation ensures that all sessions see the updated options immediately
- Post-alter hooks are invoked to allow extensions to react to the option changes

## Simplified Source

```c
static void
ATExecGenericOptions(Relation rel, List *options)
{
    Relation ftrel;
    ForeignServer *server;
    ForeignDataWrapper *fdw;
    HeapTuple tuple;
    bool isnull;
    Datum repl_val[Natts_pg_foreign_table];
    bool repl_null[Natts_pg_foreign_table];
    bool repl_repl[Natts_pg_foreign_table];
    Datum datum;
    Form_pg_foreign_table tableform;

    // Exit early if no options provided
    if (options == NIL)
        return;

    // Open pg_foreign_table catalog
    ftrel = table_open(ForeignTableRelationId, RowExclusiveLock);

    // Find the foreign table entry
    tuple = SearchSysCacheCopy1(FOREIGNTABLEREL, ObjectIdGetDatum(rel->rd_id));
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT),
                       errmsg("foreign table \"%s\" does not exist",
                              RelationGetRelationName(rel))));

    // Get FDW info for validation
    tableform = (Form_pg_foreign_table) GETSTRUCT(tuple);
    server = GetForeignServer(tableform->ftserver);
    fdw = GetForeignDataWrapper(server->fdwid);

    // Initialize replacement arrays
    memset(repl_val, 0, sizeof(repl_val));
    memset(repl_null, false, sizeof(repl_null));
    memset(repl_repl, false, sizeof(repl_repl));

    // Extract current options from catalog
    datum = SysCacheGetAttr(FOREIGNTABLEREL, tuple,
                           Anum_pg_foreign_table_ftoptions, &isnull);
    if (isnull)
        datum = PointerGetDatum(NULL);

    // Transform and validate options using FDW validator
    datum = transformGenericOptions(ForeignTableRelationId, datum, options,
                                   fdw->fdwvalidator);

    // Set up replacement values
    if (PointerIsValid(DatumGetPointer(datum)))
        repl_val[Anum_pg_foreign_table_ftoptions - 1] = datum;
    else
        repl_null[Anum_pg_foreign_table_ftoptions - 1] = true;
    repl_repl[Anum_pg_foreign_table_ftoptions - 1] = true;

    // Update the catalog tuple
    tuple = heap_modify_tuple(tuple, RelationGetDescr(ftrel),
                             repl_val, repl_null, repl_repl);
    CatalogTupleUpdate(ftrel, &tuple->t_self, tuple);

    // Invalidate caches and trigger hooks
    CacheInvalidateRelcache(rel);
    InvokeObjectPostAlterHook(ForeignTableRelationId, RelationGetRelid(rel), 0);

    // Cleanup
    table_close(ftrel, RowExclusiveLock);
    heap_freetuple(tuple);
}
```
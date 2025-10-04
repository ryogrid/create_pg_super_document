# GetForeignTable

## Location
[src/backend/foreign/foreign.c:254-291](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/foreign/foreign.c#L254-L291)

## Overview
Retrieves a foreign table definition by relation OID and constructs a complete ForeignTable structure with associated options.

## Definition
```c
ForeignTable *GetForeignTable(Oid relid)
```

## Detailed Description
GetForeignTable is a fundamental function in PostgreSQL's foreign data wrapper infrastructure that looks up foreign table metadata using the relation OID. It queries the system catalog pg_foreign_table to retrieve the table's foreign server association and options. The function constructs a ForeignTable structure containing the relation ID, associated foreign server ID, and processed table options. If the foreign table entry is not found in the system catalog, the function raises an error indicating a cache lookup failure.

## Parameters / Member Variables  
- `relid`: Object ID of the relation (foreign table) to look up

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [SysCacheGetAttr](../S/SysCacheGetAttr.md)  
  - [untransformRelOptions](../u/untransformRelOptions.md)
  - [palloc](../p/palloc.md)
  - elog
  - HeapTupleIsValid
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
- Called from (representative examples):
  - Foreign table access routines
  - FDW planning and execution functions
  - Table introspection utilities

## Notes and Other Information
The function uses the FOREIGNTABLEREL system cache for efficient lookup of foreign table metadata. The options are stored in a transformed format in the catalog and are untransformed using untransformRelOptions() to make them usable by foreign data wrappers. The ForeignTable structure returned contains essential information needed by FDWs to establish connections and query remote tables. The function is located in src/backend/foreign/foreign.c:254-291 and is crucial for foreign table operations.

## Simplified Source

```c
ForeignTable *
GetForeignTable(Oid relid)
{
    Form_pg_foreign_table tableform;
    ForeignTable *ft;
    HeapTuple tp;

    // Look up foreign table in system catalog
    tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    if (!HeapTupleIsValid(tp))
        elog(ERROR, "cache lookup failed for foreign table %u", relid);

    tableform = (Form_pg_foreign_table) GETSTRUCT(tp);

    // Create ForeignTable structure
    ft = (ForeignTable *) palloc(sizeof(ForeignTable));
    ft->relid = relid;
    ft->serverid = tableform->ftserver;

    // Extract table options
    Datum datum = SysCacheGetAttr(FOREIGNTABLEREL, tp,
                                  Anum_pg_foreign_table_ftoptions, &isnull);
    ft->options = isnull ? NIL : untransformRelOptions(datum);

    ReleaseSysCache(tp);
    return ft;
}
```
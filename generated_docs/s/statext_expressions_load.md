# statext_expressions_load

## Location
[src/backend/statistics/extended_stats.c:2405-2451](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/statistics/extended_stats.c#L2405-L2451)

## Overview
Loads a specific pg_statistic record from stored expression statistics for a given statistics object and expression index.

## Definition

```c
struct_expanded_array(eah);
```
## Detailed Description
This function retrieves expression statistics that were previously stored for extended statistics objects. It looks up the statistics object by OID in the pg_statistic_ext_data system catalog, extracts the stxdexpr field (which contains serialized expression statistics), and returns the specific pg_statistic tuple for the requested expression index. The function uses PostgreSQL's expanded array infrastructure to efficiently access individual elements from the stored array of statistics tuples.

The function performs a cache lookup to find the statistics data, extracts the expression statistics array from the stxdexpr field, and then constructs a proper HeapTuple from the stored data at the specified index. This allows the query planner and other components to access expression statistics in the same format as regular column statistics.

## Parameters / Member Variables
- : OID of the extended statistics object containing the expression statistics
- : Boolean indicating whether to load inherited statistics (for partitioned tables)
- : Zero-based index of the expression within the statistics object

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache2](../S/SearchSysCache2.md), SysCacheGetAttr, DatumGetExpandedArray
  - [deconstruct_expanded_array](../d/deconstruct_expanded_array.md), DatumGetHeapTupleHeader, HeapTupleHeaderGetDatumLength
  - [ItemPointerSetInvalid](../I/ItemPointerSetInvalid.md), heap_copytuple, ReleaseSysCache
- Called from (representative examples):
  - [examine_variable](../e/examine_variable.md)

## Notes and Other Information
- Uses the STATEXTDATASTXOID system cache for efficient lookup of statistics data
- Handles the case where expression statistics haven't been built yet by throwing an error
- The returned HeapTuple is a copy that the caller is responsible for freeing
- Essential for query planning when expressions are used in WHERE clauses or other contexts requiring selectivity estimates
- Part of PostgreSQL's extended statistics infrastructure that supports multi-column and expression statistics

## Simplified Source

```c
HeapTuple
statext_expressions_load(Oid stxoid, bool inh, int idx)
{
    bool isnull;
    Datum value;
    HeapTuple htup;

    // Look up the statistics object in pg_statistic_ext_data
    htup = SearchSysCache2(STATEXTDATASTXOID,
                          ObjectIdGetDatum(stxoid),
                          BoolGetDatum(inh));
    if (!HeapTupleIsValid(htup))
        elog(ERROR, "cache lookup failed for statistics object %u", stxoid);

    // Extract the expression statistics array from stxdexpr field
    value = SysCacheGetAttr(STATEXTDATASTXOID, htup,
                           Anum_pg_statistic_ext_data_stxdexpr, &isnull);
    if (isnull)
        elog(ERROR, "requested statistics kind \"%c\" is not yet built for statistics object %u",
             STATS_EXT_EXPRESSIONS, stxoid);

    // Get the expanded array containing expression statistics
    ExpandedArrayHeader *eah = DatumGetExpandedArray(value);
    deconstruct_expanded_array(eah);

    // Extract the specific expression's statistics tuple
    HeapTupleHeader td = DatumGetHeapTupleHeader(eah->dvalues[idx]);

    // Build a temporary HeapTuple structure
    HeapTupleData tmptup;
    tmptup.t_len = HeapTupleHeaderGetDatumLength(td);
    ItemPointerSetInvalid(&(tmptup.t_self));
    tmptup.t_tableOid = InvalidOid;
    tmptup.t_data = td;

    // Make a copy for the caller to own
    HeapTuple tup = heap_copytuple(&tmptup);

    ReleaseSysCache(htup);
    return tup;
}
```
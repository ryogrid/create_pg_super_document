# IndexGetRelation

## Location
[src/backend/catalog/index.c:3522-3546](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/index.c#L3522-L3546)

## Overview
Retrieves the OID of the table that an index is built on, given the index's relation OID, using the system cache for efficient lookup.

## Definition
```c
Oid IndexGetRelation(Oid indexId, bool missing_ok)
```

## Detailed Description
IndexGetRelation is a utility function that performs a reverse lookup from an index to its underlying table. Given an index's OID, it queries the pg_index system catalog via the system cache to retrieve the indrelid field, which contains the OID of the table the index is built on.

The function uses PostgreSQL's system cache (SearchSysCache1) for efficient lookup, avoiding direct table scans of pg_index. It includes error handling that can either throw an error or return InvalidOid when the index is not found, depending on the missing_ok parameter. An assertion verifies that the returned index entry actually matches the requested indexId.

## Parameters / Member Variables
- `indexId`: Object identifier of the index whose parent table OID is needed
- `missing_ok`: Boolean flag controlling error behavior when index is not found - if true, returns InvalidOid; if false, throws an error
## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - HeapTupleIsValid
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_index
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - InvalidOid
- Called from (representative examples):
  - [index_drop](../i/index_drop.md)
  - [reindex_index](../r/reindex_index.md)
  - [CheckIndexCompatible](../C/CheckIndexCompatible.md)
  - [bringetbitmap](../b/bringetbitmap.md)
  - [get_tables_to_cluster_partitioned](../g/get_tables_to_cluster_partitioned.md)
  - [RangeVarCallbackForReindexIndex](../R/RangeVarCallbackForReindexIndex.md)

## Notes and Other Information
- Widely used utility function throughout PostgreSQL's index and table management code
- Efficient implementation using system cache rather than direct catalog scans
- Essential for various index operations that need to identify the parent table
- Used in index dropping, reindexing, clustering, and constraint validation operations
- The missing_ok parameter provides flexibility for callers that may encounter missing indexes during concurrent operations
- Returns the actual table OID stored in pg_index.indrelid field
- Includes safety assertion to verify cache consistency

## Simplified Source

```c
Oid IndexGetRelation(Oid indexId, bool missing_ok) {
    HeapTuple tuple;
    Form_pg_index index;
    Oid result;

    // Look up index in system cache
    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(indexId));

    if (!HeapTupleIsValid(tuple)) {
        if (missing_ok)
            return InvalidOid;
        elog(ERROR, "cache lookup failed for index %u", indexId);
    }

    // Extract the parent table OID
    index = (Form_pg_index) GETSTRUCT(tuple);
    result = index->indrelid;

    ReleaseSysCache(tuple);
    return result;
}
```
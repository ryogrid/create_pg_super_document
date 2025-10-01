# get_index_isreplident

## Location
[src/backend/utils/cache/lsyscache.c:3555-3577](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/lsyscache.c#L3555-L3577)

## Overview
Checks whether a given index is marked as a replica identity index, which is used for logical replication to uniquely identify rows in replicated tables.

## Definition
```c
bool get_index_isreplident(Oid index_oid)
```

## Detailed Description
This function queries the pg_index system catalog to retrieve the indisreplident flag for a specified index. The replica identity index is crucial for logical replication in PostgreSQL - it determines which index should be used to identify rows when applying changes on replica servers.

When a table is configured for logical replication, PostgreSQL needs a way to uniquely identify rows for UPDATE and DELETE operations. The replica identity index serves this purpose. Only one index per table can be marked as the replica identity index, and it must be unique and not partial.

## Parameters / Member Variables
- `index_oid`: The OID of the index to check for replica identity status

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md) (system catalog lookup with INDEXRELID)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple data extraction)
  - [ReleaseSysCache](../R/ReleaseSysCache.md) (cache cleanup)
  - Form_pg_index (pg_index catalog structure)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md) (OID to Datum conversion)
- Called from (representative examples):
  - [RememberReplicaIdentityForRebuilding](../R/RememberReplicaIdentityForRebuilding.md)

## Notes and Other Information
- Returns false if the index OID is invalid or not found
- Returns the boolean value of the indisreplident field from pg_index
- Used primarily in table alteration operations involving replica identity
- Essential for logical replication functionality
- The replica identity index must be unique and cannot be a partial index
- Only one index per table can have indisreplident set to true
- Related to ALTER TABLE ... REPLICA IDENTITY commands

## Simplified Source

```c
bool
get_index_isreplident(Oid index_oid)
{
    HeapTuple tuple;
    Form_pg_index rd_index;
    bool result;

    // Look up the index in pg_index catalog
    tuple = SearchSysCache1(INDEXRELID, ObjectIdGetDatum(index_oid));
    if (!HeapTupleIsValid(tuple))
        return false;

    // Extract the indisreplident flag
    rd_index = (Form_pg_index) GETSTRUCT(tuple);
    result = rd_index->indisreplident;
    ReleaseSysCache(tuple);

    return result;
}
```
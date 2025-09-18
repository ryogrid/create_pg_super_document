# get_index_isreplident

## Location
src/backend/utils/cache/lsyscache.c: 3555 - 3577

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
  - SearchSysCache1 (system catalog lookup with INDEXRELID)
  - HeapTupleIsValid (tuple validation)
  - GETSTRUCT (tuple data extraction)
  - ReleaseSysCache (cache cleanup)
  - Form_pg_index (pg_index catalog structure)
  - ObjectIdGetDatum (OID to Datum conversion)
- Called from (representative examples):
  - RememberReplicaIdentityForRebuilding

## Notes and Other Information
- Returns false if the index OID is invalid or not found
- Returns the boolean value of the indisreplident field from pg_index
- Used primarily in table alteration operations involving replica identity
- Essential for logical replication functionality
- The replica identity index must be unique and cannot be a partial index
- Only one index per table can have indisreplident set to true
- Related to ALTER TABLE ... REPLICA IDENTITY commands
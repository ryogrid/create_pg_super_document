# IndexGetRelation

## Location
src/backend/catalog/index.c: 3522 - 3546

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
- : Object identifier of the index whose parent table OID is needed
- : Boolean flag controlling error behavior when index is not found - if true, returns InvalidOid; if false, throws an error

## Dependencies
- Functions called/Symbols referenced:
  - SearchSysCache1
  - HeapTupleIsValid
  - GETSTRUCT
  - ReleaseSysCache
  - Form_pg_index
  - ObjectIdGetDatum
  - InvalidOid
- Called from (representative examples):
  - index_drop
  - reindex_index
  - CheckIndexCompatible
  - bringetbitmap
  - get_tables_to_cluster_partitioned
  - RangeVarCallbackForReindexIndex

## Notes and Other Information
- Widely used utility function throughout PostgreSQL's index and table management code
- Efficient implementation using system cache rather than direct catalog scans
- Essential for various index operations that need to identify the parent table
- Used in index dropping, reindexing, clustering, and constraint validation operations
- The missing_ok parameter provides flexibility for callers that may encounter missing indexes during concurrent operations
- Returns the actual table OID stored in pg_index.indrelid field
- Includes safety assertion to verify cache consistency
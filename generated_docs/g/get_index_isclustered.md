# get_index_isclustered

## Location
src/backend/utils/cache/lsyscache.c: 3601 - 3624

## Overview
Returns whether a given index is marked as clustered in the PostgreSQL system catalog pg_index.

## Definition


## Detailed Description
This function performs a lookup in the PostgreSQL system cache to determine if a specific index is marked as clustered. A clustered index indicates that the table data is physically ordered according to the index's key order. The function accesses the pg_index system catalog through the system cache mechanism for efficient retrieval of the indisclustered field.

The function uses the system cache (INDEXRELID cache) to find the index's metadata and extracts the indisclustered boolean field from the Form_pg_index structure. If the index OID is invalid or not found, the function raises an ERROR.

## Parameters / Member Variables
- : The object identifier (OID) of the index to check for clustering status

## Dependencies
- Functions called/Symbols referenced:
  - [SearchSysCache1](../S/SearchSysCache1.md)
  - [ObjectIdGetDatum](../O/ObjectIdGetDatum.md)
  - HeapTupleIsValid
  - elog
  - GETSTRUCT
  - [ReleaseSysCache](../R/ReleaseSysCache.md)
  - Form_pg_index
- Called from (representative examples):
  - [cluster](../c/cluster.md)
  - [cluster_rel](../c/cluster_rel.md)
  - [mark_index_clustered](../m/mark_index_clustered.md)
  - [RememberClusterOnForRebuilding](../R/RememberClusterOnForRebuilding.md)

## Notes and Other Information
- This function is part of the low-level system cache interface (lsyscache) that provides efficient access to system catalog information
- The function will throw an ERROR if the provided index_oid is not found in the system catalog
- Clustered indexes affect query optimization and physical storage layout decisions
- Located in src/backend/utils/cache/lsyscache.c:3601-3624
# CacheInvalidateCatalog

## Location
src/backend/utils/cache/inval.c: 1339 - 1362

## Overview
Registers invalidation of the whole content of a system catalog, typically used during VACUUM FULL/CLUSTER operations when tuples have been moved around.

## Definition


## Detailed Description
CacheInvalidateCatalog is used to invalidate all cached entries for an entire system catalog. This function is primarily called during VACUUM FULL and CLUSTER operations where tuples haven't been changed per se, but have been physically moved to different locations. Since some cache entries depend on correct TIDs (tuple identifiers), all entries for the catalog must be invalidated when the physical layout changes.

The function determines whether the catalog is shared across databases or specific to the current database, then registers the appropriate invalidation message. For shared catalogs (like pg_database, pg_authid), the invalidation applies across all databases. For regular catalogs, it only applies to the current database.

## Parameters / Member Variables
- : The OID of the system catalog relation to invalidate

## Dependencies
- Functions called/Symbols referenced:
  - PrepareInvalidationState
  - IsSharedRelation
  - RegisterCatalogInvalidation
- Called from (representative examples):
  - finish_heap_swap

## Notes and Other Information
- The caller is expected to verify that the relation is actually a system catalog, though no harm occurs if it isn't (just a wasted invalidation message)
- The function handles both shared and non-shared catalogs appropriately by setting the database ID to InvalidOid for shared relations
- This is a more heavyweight operation compared to invalidating individual tuples, as it invalidates the entire catalog content
# InitCatalogCache

## Location
src/backend/utils/cache/syscache.c: 110 - 179

## Overview
Initializes PostgreSQL's system catalog cache infrastructure by allocating memory and setting up cache structures without performing any database access.

## Definition


## Detailed Description
InitCatalogCache is responsible for the initial setup of PostgreSQL's system catalog cache system. It iterates through all predefined cache configurations in the cacheinfo array and creates individual catalog caches using InitCatCache(). The function performs several critical tasks:

1. Validates that all cache configurations have valid relation and index OIDs
2. Creates individual catalog caches for each system catalog
3. Builds arrays of relation OIDs used by the cache system
4. Sorts and deduplicates OID arrays for efficient binary search operations
5. Sets the global CacheInitialized flag to true

The function operates in a "lazy initialization" model where actual database interrogation is deferred until the first use of each cache, making the startup process more efficient.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [InitCatCache](InitCatCache.md)
  - PointerIsValid
  - [RelationInvalidatesSnapshotsOnly](../R/RelationInvalidatesSnapshotsOnly.md)
  - lengthof
  - qsort
  - [oid_compare](../o/oid_compare.md)
  - [qunique](../q/qunique.md)
- Called from (representative examples):
  - [InitPostgres](InitPostgres.md)

## Notes and Other Information
- Must be called exactly once during PostgreSQL backend initialization
- The function asserts that CacheInitialized is false at entry
- Database access is explicitly avoided during this phase - actual cache population happens on first use
- The function maintains two sorted OID arrays (SysCacheRelationOid and SysCacheSupportingRelOid) for efficient cache invalidation
- All cache configurations must have valid relation and index OIDs, which is enforced through assertions
- The sorting and deduplication of OID arrays enables binary search for cache invalidation operations
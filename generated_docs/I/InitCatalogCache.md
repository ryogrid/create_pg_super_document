# InitCatalogCache

## Location
[src/backend/utils/cache/syscache.c:110-179](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/syscache.c#L110-L179)

## Overview
Initializes PostgreSQL's system catalog cache infrastructure by allocating memory and setting up cache structures without performing any database access.

## Definition

```c
enumeration value defined in syscache.h has been
		 * populated in the cacheinfo array.
		 */
		Assert(OidIsValid(cacheinfo[cacheId].reloid));
```
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

## Simplified Source

```c
// Simplified version of InitCatalogCache
void InitCatalogCache(void) {
    int cacheId;

    Assert(!CacheInitialized);

    // Initialize OID array sizes
    SysCacheRelationOidSize = SysCacheSupportingRelOidSize = 0;

    // Initialize each system catalog cache
    for (cacheId = 0; cacheId < SysCacheSize; cacheId++) {
        // Validate cache configuration
        Assert(OidIsValid(cacheinfo[cacheId].reloid));
        Assert(OidIsValid(cacheinfo[cacheId].indoid));

        // Create the catalog cache
        SysCache[cacheId] = InitCatCache(cacheId,
                                         cacheinfo[cacheId].reloid,
                                         cacheinfo[cacheId].indoid,
                                         cacheinfo[cacheId].nkeys,
                                         cacheinfo[cacheId].key,
                                         cacheinfo[cacheId].nbuckets);

        // Check for initialization failure
        if (!PointerIsValid(SysCache[cacheId]))
            elog(ERROR, "could not initialize cache %u (%d)",
                 cacheinfo[cacheId].reloid, cacheId);

        // Build OID arrays for cache invalidation
        SysCacheRelationOid[SysCacheRelationOidSize++] = cacheinfo[cacheId].reloid;
        SysCacheSupportingRelOid[SysCacheSupportingRelOidSize++] = cacheinfo[cacheId].reloid;
        SysCacheSupportingRelOid[SysCacheSupportingRelOidSize++] = cacheinfo[cacheId].indoid;

        Assert(!RelationInvalidatesSnapshotsOnly(cacheinfo[cacheId].reloid));
    }

    // Validate array bounds
    Assert(SysCacheRelationOidSize <= lengthof(SysCacheRelationOid));
    Assert(SysCacheSupportingRelOidSize <= lengthof(SysCacheSupportingRelOid));

    // Sort and deduplicate OID arrays for binary search
    qsort(SysCacheRelationOid, SysCacheRelationOidSize, sizeof(Oid), oid_compare);
    SysCacheRelationOidSize = qunique(SysCacheRelationOid, SysCacheRelationOidSize,
                                      sizeof(Oid), oid_compare);

    qsort(SysCacheSupportingRelOid, SysCacheSupportingRelOidSize, sizeof(Oid), oid_compare);
    SysCacheSupportingRelOidSize = qunique(SysCacheSupportingRelOid, SysCacheSupportingRelOidSize,
                                           sizeof(Oid), oid_compare);

    // Mark cache system as initialized
    CacheInitialized = true;
}
```

Key simplifications made:
- Added clear comments for each major operation section
- Removed detailed intermediate comments while preserving essential logic
- Grouped related operations together with explanatory comments
- Maintained all critical assertions and error handling
- Preserved the complete initialization flow including OID array management
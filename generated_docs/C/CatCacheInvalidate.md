# CatCacheInvalidate

## Location
[src/backend/utils/cache/catcache.c:625-707](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L625-L707)

## Overview
CatCacheInvalidate is a public function that invalidates cache entries in a specified catalog cache based on a hash value, handling both active and in-progress cache entries.

## Definition

```c
void
CatCacheInvalidate(CatCache *cache, uint32 hashValue)
```
## Detailed Description
This function performs selective invalidation of catalog cache entries based on hash values rather than tuple identifiers (TIDs). The design choice to use hash values instead of TIDs addresses a critical safety issue: after VACUUM FULL operations on system catalogs, the same logical tuple may have a different TID, making TID-based invalidation unreliable.

The function operates in several phases:
1. **CatCList Invalidation**: All cache lists in the target cache are invalidated because determining which lists remain valid after a catalog change is computationally expensive
2. **Hash-based Entry Invalidation**: Entries matching the provided hash value are either marked as dead (if referenced) or immediately removed
3. **In-progress Entry Invalidation**: Any cache entries currently being constructed are also invalidated if they match the criteria

The function accepts the small risk of false positive invalidations due to hash collisions in exchange for guaranteed safety and simplicity.

## Parameters / Member Variables
- : Pointer to the CatCache structure containing entries to be invalidated
- : The hash value used to identify which cache entries should be invalidated

## Dependencies
- Functions called/Symbols referenced:
  - CACHE_elog (for debug logging)
  - dlist_foreach_modify (for safe iteration during modification)
  - dlist_container (for extracting structures from list nodes)
  - [CatCacheRemoveCList](CatCacheRemoveCList.md) (for removing unreferenced cache lists)
  - HASH_INDEX (macro for computing bucket index)
  - [CatCacheRemoveCTup](CatCacheRemoveCTup.md) (for removing unreferenced cache entries)
- Called from (representative examples):
  - [SysCacheInvalidate](../S/SysCacheInvalidate.md) (system cache invalidation interface)

## Notes and Other Information
- Function is declared as public but intended to be quasi-public, primarily used by inval.c
- Uses hash-based invalidation instead of TID-based for safety after VACUUM FULL operations
- Marks entries as "dead" rather than immediately removing them if they have active references
- Invalidates all cache lists rather than attempting selective invalidation for performance reasons
- Includes debug logging when CACHE_elog is enabled
- Updates invalidation statistics when CATCACHE_STATS is compiled in
- Handles the global catcache_in_progress_stack to invalidate entries being built concurrently
- Critical component of PostgreSQL's cache coherency system during catalog modifications
# cachedNamespacePath

## Location
[src/backend/catalog/namespace.c:4244-4298](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L4244-L4298)

## Overview
Retrieves search path information from a cache, computing missing components if needed. This function optimizes repeated namespace path lookups by caching preprocessed results.

## Definition

```c
static const SearchPathCacheEntry *
cachedNamespacePath(const char *searchPath, Oid roleid)
```
## Detailed Description
The cachedNamespacePath function implements a caching mechanism for PostgreSQL's namespace search path resolution. It first initializes the search path cache and attempts to retrieve an existing cache entry for the given search path and role ID. If the cache entry exists but is missing components (due to previous out-of-memory conditions), it computes the missing parts.

The function handles two main cached components:
1. **oidlist**: A preprocessed list of namespace OIDs derived from the search path string
2. **finalPath**: The final resolved namespace path after applying access controls and hooks

When object access hooks are present, the finalPath must be recomputed each time to ensure hook effects are properly applied, though this is still more efficient than full string reprocessing.

## Parameters / Member Variables
- : The namespace search path string to resolve
- : The OID of the role for which to resolve the search path

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathCacheEntry](../S/SearchPathCacheEntry.md) (struct type)
  - [spcache_init](../s/spcache_init.md)
  - [spcache_insert](../s/spcache_insert.md) 
  - [preprocessNamespacePath](../p/preprocessNamespacePath.md)
  - [list_free](../l/list_free.md)
  - [finalNamespacePath](../f/finalNamespacePath.md)
- Called from (representative examples):
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md)

## Notes and Other Information
- The function uses SearchPathCacheContext memory context for cached data to ensure proper memory management
- Cache entries may have missing components due to previous OOM conditions, which this function handles gracefully
- When object_access_hook is set, the forceRecompute flag ensures consistent behavior across calls
- The returned cache entry is only valid until the next call to this function
- This is a static function, only accessible within the namespace.c compilation unit

## Simplified Source

```c
static const SearchPathCacheEntry *
cachedNamespacePath(const char *searchPath, Oid roleid)
{
    MemoryContext oldcxt;
    SearchPathCacheEntry *entry;

    // Initialize cache if needed
    spcache_init();

    // Get or create cache entry
    entry = spcache_insert(searchPath, roleid);

    // Compute missing oidlist (may be missing due to OOM)
    if (entry->oidlist == NIL) {
        oldcxt = MemoryContextSwitchTo(SearchPathCacheContext);
        entry->oidlist = preprocessNamespacePath(searchPath, roleid,
                                                &entry->temp_missing);
        MemoryContextSwitchTo(oldcxt);
    }

    // Recompute finalPath if missing or if hooks might affect result
    if (entry->finalPath == NIL || object_access_hook || entry->forceRecompute) {
        // Clean up old finalPath
        list_free(entry->finalPath);
        entry->finalPath = NIL;

        // Compute new finalPath from oidlist
        oldcxt = MemoryContextSwitchTo(SearchPathCacheContext);
        entry->finalPath = finalNamespacePath(entry->oidlist, &entry->firstNS);
        MemoryContextSwitchTo(oldcxt);

        // Mark for recomputation if hooks are active
        entry->forceRecompute = object_access_hook ? true : false;
    }

    return entry;
}
```
# SearchPathCacheEntry

## Location
src/backend/catalog/namespace.c: 173 - 184

## Overview
SearchPathCacheEntry is a struct that represents a cached entry in PostgreSQL's search path cache, storing computed namespace information and metadata for efficient namespace resolution.

## Definition
```c
typedef struct SearchPathCacheEntry
{
    SearchPathCacheKey key;
    List       *oidlist;        /* namespace OIDs that pass ACL checks */
    List       *finalPath;      /* cached final computed search path */
    Oid         firstNS;        /* first explicitly-listed namespace */
    bool        temp_missing;
    bool        forceRecompute; /* force recompute of finalPath */
    
    /* needed for simplehash */
    char        status;
} SearchPathCacheEntry;
```

## Detailed Description
SearchPathCacheEntry is the value type stored in PostgreSQL's search path hash table cache. Each entry caches the results of expensive namespace resolution computations, including the list of accessible namespace OIDs, the final computed search path, and various metadata about the search path state. This caching mechanism significantly improves performance by avoiding redundant access control checks and path computations for frequently used search path configurations.

The cache entry maintains both the original search path key (searchPath + roleid) and the computed results, allowing for quick lookup and reuse of previously calculated namespace information. The cache is particularly beneficial when dealing with complex search paths or when the same search path is used repeatedly across multiple queries.

## Parameters / Member Variables
- `key`: SearchPathCacheKey containing the search path string and role ID that uniquely identifies this cache entry
- `oidlist`: List of namespace OIDs that the role has access to based on ACL checks
- `finalPath`: The final computed search path after resolving all namespace references and access controls
- `firstNS`: OID of the first explicitly-listed namespace in the search path
- `temp_missing`: Boolean flag indicating whether temporary namespace resolution failed or is missing
- `forceRecompute`: Boolean flag that forces recomputation of the finalPath when set to true
- `status`: Character field required by the simplehash hash table implementation for entry state tracking

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathCacheKey](SearchPathCacheKey.md) (embedded as the key field)
- Called from (representative examples):
  - [spcache_lookup](../s/spcache_lookup.md) (when retrieving cached entries)
  - [spcache_insert](../s/spcache_insert.md) (when creating new cache entries)
  - [finalNamespacePath](../f/finalNamespacePath.md) (when accessing cached final path)
  - [cachedNamespacePath](../c/cachedNamespacePath.md) (when retrieving cached namespace path)
  - [recomputeNamespacePath](../r/recomputeNamespacePath.md) (when updating cached computations)

## Notes and Other Information
- This struct is used as the SH_ELEMENT_TYPE in the simplehash implementation for the search path cache
- The cache has a reset threshold of 256 entries (SPCACHE_RESET_THRESHOLD) to prevent unbounded memory growth
- The status field is required by the simplehash hash table library for tracking entry states
- The forceRecompute flag allows selective invalidation of cached results without removing the entire entry
- Located in src/backend/catalog/namespace.c:173-184
- Part of PostgreSQL's namespace resolution optimization system that significantly improves query performance
# spcache_lookup

## Location
[src/backend/catalog/namespace.c:344-373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L344-L373)

## Overview
Searches for an existing entry in the search path cache without inserting new entries, providing optimized lookup with a last-entry optimization for repeated access patterns.

## Definition
```c
static SearchPathCacheEntry *spcache_lookup(const char *searchPath, Oid roleid)
```

## Detailed Description
This function implements an optimized lookup mechanism for PostgreSQL's search path cache. It employs a two-tier lookup strategy: first checking if the requested entry matches the last accessed cache entry (LastSearchPathCacheEntry optimization), and if not, performing a full hash table lookup using nsphash_lookup. This design leverages temporal locality - the assumption that recently accessed cache entries are likely to be accessed again soon. When a successful lookup occurs via the hash table, the function updates the last-entry pointer to optimize future lookups. The function returns NULL if no matching entry is found, maintaining a clear interface for cache miss scenarios.

## Parameters / Member Variables
- `searchPath`: The search path string to look up in the cache (const char pointer)
- `roleid`: The role ID (Oid) associated with the search path for role-specific caching

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathCacheEntry](../S/SearchPathCacheEntry.md) (cache entry structure type)
  - [SearchPathCacheKey](../S/SearchPathCacheKey.md) (key structure for hash table operations)
  - nsphash_lookup (hash table lookup function)
  - strcmp (standard C library string comparison)
- Called from (representative examples):
  - [check_search_path](../c/check_search_path.md) (for search path validation operations)

## Notes and Other Information
- Implements a performance optimization through LastSearchPathCacheEntry caching
- Non-mutating operation that only performs lookups without insertions
- Part of PostgreSQL's namespace system optimization for search path operations
- Uses both role ID and search path string for precise cache key matching
- Returns NULL for cache misses, allowing callers to handle insertion separately
- Updates the last-entry cache pointer on successful hash table lookups to improve future performance
# spcache_insert

## Location
src/backend/catalog/namespace.c: 374 - 440

## Overview
Looks up or inserts a new entry in PostgreSQL's search path cache, handling both cache hits and the creation of new cache entries with proper memory management and initialization.

## Definition
```c
static SearchPathCacheEntry *spcache_insert(const char *searchPath, Oid roleid)
```

## Detailed Description
This function implements a lookup-or-insert operation for PostgreSQL's search path cache with careful attention to memory management and performance optimization. Like spcache_lookup, it first checks the LastSearchPathCacheEntry optimization for repeated access patterns. If no match is found, it performs a hash table lookup to check for existing entries. When a new entry must be created, the function safely handles memory allocation by using MemoryContextStrdup to create a persistent copy of the search path string in the SearchPathCacheContext. The function initializes all fields of new cache entries to safe default values, ensuring the cache entry is fully initialized before returning. The implementation prioritizes safety against out-of-memory conditions by validating the key before insertion.

## Parameters / Member Variables
- `searchPath`: The search path string to lookup or insert (const char pointer)
- `roleid`: The role ID (Oid) associated with the search path for role-specific caching

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathCacheEntry](../S/SearchPathCacheEntry.md) (cache entry structure type)
  - [SearchPathCacheKey](../S/SearchPathCacheKey.md) (key structure for hash table operations)  
  - nsphash_lookup (hash table lookup function)
  - nsphash_insert (hash table insertion function)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (creates persistent string copy in cache context)
  - strcmp (standard C library string comparison)
  - Assert (assertion macro for debugging)
- Called from (representative examples):
  - [cachedNamespacePath](../c/cachedNamespacePath.md) (for namespace path caching operations)
  - [check_search_path](../c/check_search_path.md) (for search path validation and caching)

## Notes and Other Information
- Combines lookup and insertion logic for efficient cache management
- Uses lazy string duplication - only copies the search path string if a new entry is needed
- Initializes new cache entries with safe default values (NIL lists, InvalidOid, false flags)
- Implements the same LastSearchPathCacheEntry optimization as spcache_lookup
- Handles memory allocation safely to prevent OOM issues from creating invalid cache entries
- Does not touch the entry->status field as it's managed by the simplehash implementation
- Returns a valid SearchPathCacheEntry pointer in all successful cases, never NULL
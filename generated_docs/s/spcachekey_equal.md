# spcachekey_equal

## Location
[src/backend/catalog/namespace.c:274-279](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/catalog/namespace.c#L274-L279)

## Overview
Equality comparison function that determines if two SearchPathCacheKey structures are identical, used in PostgreSQL's search path caching system for hash table key comparison.

## Definition
```c
static inline bool spcachekey_equal(SearchPathCacheKey a, SearchPathCacheKey b)
```

## Detailed Description
This function implements the equality comparison logic for SearchPathCacheKey structures in PostgreSQL's search path cache. It performs a two-part comparison: first checking if the role IDs are equal, then using string comparison to verify that the search path strings are identical. The function is marked as static inline for performance optimization since it's called frequently during hash table operations. This equality function is essential for the hash table implementation to correctly identify matching cache entries.

## Parameters / Member Variables
- `a`: First SearchPathCacheKey structure to compare, containing roleid and searchPath components
- `b`: Second SearchPathCacheKey structure to compare, containing roleid and searchPath components

## Dependencies
- Functions called/Symbols referenced:
  - [SearchPathCacheKey](../S/SearchPathCacheKey.md) (key structure type)
  - strcmp (standard C library string comparison function)
- Called from (representative examples):
  - SH_EQUAL macro (used in simplehash hash table implementation)

## Notes and Other Information
- Part of the search path cache implementation that optimizes namespace operations
- Uses both role ID and search path string comparison to ensure complete key equality
- The function returns true only if both the role ID and search path string match exactly
- Essential component for hash table collision resolution in the search path cache
- Performance-critical function that's inlined for efficiency in hash table operations

## Simplified Source

```c
static inline bool spcachekey_equal(SearchPathCacheKey a, SearchPathCacheKey b)
{
    // Compare both role ID and search path string
    return a.roleid == b.roleid &&
           strcmp(a.searchPath, b.searchPath) == 0;
}
```
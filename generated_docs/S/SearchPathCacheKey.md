# SearchPathCacheKey

## Location
src/backend/catalog/namespace.c: 167 - 171

## Overview
SearchPathCacheKey is a struct used as the key in PostgreSQL's search path caching system to uniquely identify cached search path computations based on the search path string and role ID.

## Definition
```c
typedef struct SearchPathCacheKey
{
    const char *searchPath;
    Oid         roleid;
} SearchPathCacheKey;
```

## Detailed Description
SearchPathCacheKey serves as the lookup key in PostgreSQL's search path caching mechanism. The search path cache optimizes namespace resolution by storing previously computed search paths to avoid redundant calculations. This struct combines two essential pieces of information that uniquely identify a search path configuration: the actual search path string and the role (user) ID, since different roles may have different access permissions to the same namespaces.

The caching system is crucial for performance as namespace resolution can be expensive, especially when dealing with complex search paths or when access control checks need to be performed repeatedly.

## Parameters / Member Variables
- `searchPath`: A string representing the search path configuration (e.g., "public,schema1,schema2")
- `roleid`: The OID of the role/user for which this search path applies, since access permissions vary by role

## Dependencies
- Functions called/Symbols referenced:
  - None (this is a simple data structure)
- Called from (representative examples):
  - [spcachekey_hash](../s/spcachekey_hash.md) (for hash table operations)
  - [spcachekey_equal](../s/spcachekey_equal.md) (for hash table key comparison)
  - [spcache_lookup](../s/spcache_lookup.md) (when searching for cached entries)
  - [spcache_insert](../s/spcache_insert.md) (when inserting new cache entries)

## Notes and Other Information
- This struct is used as the SH_KEY_TYPE in the simplehash implementation for the search path cache
- The searchPath field is a const char pointer, indicating the string should not be modified through this reference
- The combination of searchPath and roleid ensures that cached results are role-specific, maintaining proper access control semantics
- Part of PostgreSQL's namespace resolution optimization system located in src/backend/catalog/namespace.c:167-171
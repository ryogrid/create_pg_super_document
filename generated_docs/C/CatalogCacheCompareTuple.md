# CatalogCacheCompareTuple

## Location
[src/backend/utils/cache/catcache.c:441-459](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/catcache.c#L441-L459)

## Overview
A static inline function that efficiently compares cached tuple key values against search key values using fast equality functions for catalog cache lookups.

## Definition

```c
static inline bool
CatalogCacheCompareTuple(const CatCache *cache, int nkeys,
						 const Datum *cachekeys,
						 const Datum *searchkeys)
```
## Detailed Description
The `CatalogCacheCompareTuple` function performs optimized key-by-key comparison between cached tuple keys and search keys during catalog cache lookups. It iterates through each key position and uses the corresponding fast equality function from the cache's `cc_fastequal` array to compare the values. The function returns `true` only if all key pairs match, and `false` as soon as any mismatch is found (short-circuit evaluation). This function is critical for catalog cache performance as it's called during every cache search operation. The use of fast equality functions (set up by `GetCCHashEqFuncs`) provides type-specific optimizations that are much faster than the general-purpose PostgreSQL equality operators.

## Parameters / Member Variables
- `cache`: Const pointer to the CatCache structure containing fast equality function pointers
- `nkeys`: Number of key values to compare
- `cachekeys`: Array of Datum values representing the cached tuple's key values
- `searchkeys`: Array of Datum values representing the search criteria

## Dependencies
- Functions called/Symbols referenced:
  - `[CatCache](CatCache.md)`: Structure type containing the fast equality function array (`cc_fastequal`)
- Called from (representative examples):
  - [SearchCatCacheInternal](../S/SearchCatCacheInternal.md): Used during primary catalog cache search operations
  - [SearchCatCacheList](../S/SearchCatCacheList.md): Used during list-based catalog cache searches

## Notes and Other Information
- Declared as `static inline` for maximum performance since it's called frequently during cache searches
- Uses short-circuit evaluation: returns `false` immediately upon finding the first non-matching key
- Leverages fast equality functions that are type-specific optimizations much faster than standard SQL equality
- The fast equality functions are set up during cache initialization by `GetCCHashEqFuncs`
- Essential for the inner loop of catalog cache searches where performance is critical
- Supports variable number of keys (1-4) as determined by the specific catalog cache configuration

## Simplified Source

```c
static inline bool CatalogCacheCompareTuple(const CatCache *cache, int nkeys,
                                          const Datum *cachekeys, const Datum *searchkeys) {
    const CCFastEqualFN *cc_fastequal = cache->cc_fastequal;

    // Compare each key pair using fast equality functions
    for (int i = 0; i < nkeys; i++) {
        if (!cc_fastequal[i](cachekeys[i], searchkeys[i]))
            return false;  // Mismatch found
    }

    return true;  // All keys match
}
```
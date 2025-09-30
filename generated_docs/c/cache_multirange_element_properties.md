# cache_multirange_element_properties

## Location
[src/backend/utils/cache/typcache.c:1682-1709](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1682-L1709)

## Overview
Caches hash function properties for the element type of a multirange type, determining whether the element type supports regular and extended hashing.

## Definition
```c
static void cache_multirange_element_properties(TypeCacheEntry *typentry)
```

## Detailed Description
This function populates the type cache entry with information about the hashing capabilities of a multirange type's element type. It first ensures that the multirange type information is loaded by calling load_multirangetype_info() if needed. For multirange types, it accesses the element type through the associated range type (typentry->rngtype->rngelemtype). It then looks up the element type's cache entry to check for hash_proc and hash_extended_proc functions. Based on the availability of these hash functions, it sets the appropriate flags (TCFLAGS_HAVE_ELEM_HASHING and TCFLAGS_HAVE_ELEM_EXTENDED_HASHING) in the type cache entry. Finally, it marks that element properties have been checked by setting TCFLAGS_CHECKED_ELEM_PROPERTIES.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry for the multirange type whose element properties need to be cached

## Dependencies
- Functions called/Symbols referenced:
  - [load_multirangetype_info](../l/load_multirangetype_info.md)
  - [lookup_type_cache](../l/lookup_type_cache.md)
  - TYPTYPE_MULTIRANGE (type constant)
  - TYPECACHE_HASH_PROC (cache flag)
  - TYPECACHE_HASH_EXTENDED_PROC (cache flag)
  - TCFLAGS_HAVE_ELEM_HASHING (flag constant)
  - TCFLAGS_HAVE_ELEM_EXTENDED_HASHING (flag constant)
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag constant)
- Called from (representative examples):
  - [multirange_element_has_hashing](../m/multirange_element_has_hashing.md)
  - [multirange_element_has_extended_hashing](../m/multirange_element_has_extended_hashing.md)

## Notes and Other Information
This is a static helper function that implements lazy initialization for multirange element type properties. It follows the same pattern as cache_range_element_properties but handles the more complex indirection required for multirange types (multirange -> range -> element). The function ensures that all necessary type information is loaded before attempting to access element properties, making it robust for use in various contexts within the type cache system. The multirange type system was introduced in PostgreSQL 14 to represent collections of non-overlapping ranges.

## Simplified Source

```c
static void cache_multirange_element_properties(TypeCacheEntry *typentry) {
    // Load multirange info if needed
    if (typentry->rngtype == NULL && typentry->typtype == TYPTYPE_MULTIRANGE)
        load_multirangetype_info(typentry);

    // Check element hashing capabilities via range type
    if (typentry->rngtype != NULL && typentry->rngtype->rngelemtype != NULL) {
        TypeCacheEntry *elementry = lookup_type_cache(
            typentry->rngtype->rngelemtype->type_id,
            TYPECACHE_HASH_PROC | TYPECACHE_HASH_EXTENDED_PROC);

        if (OidIsValid(elementry->hash_proc))
            typentry->flags |= TCFLAGS_HAVE_ELEM_HASHING;
        if (OidIsValid(elementry->hash_extended_proc))
            typentry->flags |= TCFLAGS_HAVE_ELEM_EXTENDED_HASHING;
    }

    typentry->flags |= TCFLAGS_CHECKED_ELEM_PROPERTIES;
}
```
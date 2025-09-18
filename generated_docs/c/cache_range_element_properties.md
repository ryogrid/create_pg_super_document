# cache_range_element_properties

## Location
src/backend/utils/cache/typcache.c: 1642 - 1665

## Overview
Caches hash function properties for the element type of a range type, determining whether the element type supports regular and extended hashing.

## Definition
```c
static void cache_range_element_properties(TypeCacheEntry *typentry)
```

## Detailed Description
This function populates the type cache entry with information about the hashing capabilities of a range type's element type. It first ensures that the range element type information is loaded by calling load_rangetype_info() if needed. Then it looks up the element type's cache entry to check for hash_proc and hash_extended_proc functions. Based on the availability of these hash functions, it sets the appropriate flags (TCFLAGS_HAVE_ELEM_HASHING and TCFLAGS_HAVE_ELEM_EXTENDED_HASHING) in the type cache entry. Finally, it marks that element properties have been checked by setting TCFLAGS_CHECKED_ELEM_PROPERTIES.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry for the range type whose element properties need to be cached

## Dependencies
- Functions called/Symbols referenced:
  - load_rangetype_info
  - lookup_type_cache
  - TYPTYPE_RANGE (type constant)
  - TYPECACHE_HASH_PROC (cache flag)
  - TYPECACHE_HASH_EXTENDED_PROC (cache flag)
  - TCFLAGS_HAVE_ELEM_HASHING (flag constant)
  - TCFLAGS_HAVE_ELEM_EXTENDED_HASHING (flag constant)
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag constant)
- Called from (representative examples):
  - range_element_has_hashing
  - range_element_has_extended_hashing

## Notes and Other Information
This is a static helper function that implements lazy initialization for range element type properties. It only performs the work when the TCFLAGS_CHECKED_ELEM_PROPERTIES flag is not set, ensuring that expensive lookups are done only once. The function handles the case where the range element type information may not be loaded yet, making it robust for use in various contexts within the type cache system.
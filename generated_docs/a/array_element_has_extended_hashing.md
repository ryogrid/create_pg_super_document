# array_element_has_extended_hashing

## Location
src/backend/utils/cache/typcache.c: 1451 - 1458

## Overview
Determines whether an array type has extended hash function support available for its element type.

## Definition
```c
static bool array_element_has_extended_hashing(TypeCacheEntry *typentry)
```

## Detailed Description
This function checks if the array element type associated with a given type cache entry has extended hashing functionality available. Extended hashing refers to support for 64-bit hash values and more sophisticated hash algorithms beyond the basic 32-bit hash functions. Like its companion functions, it first ensures that element properties have been cached by checking the `TCFLAGS_CHECKED_ELEM_PROPERTIES` flag, and if not, it calls `cache_array_element_properties()` to populate the cache. Once the properties are cached, it returns whether the `TCFLAGS_HAVE_ELEM_EXTENDED_HASHING` flag is set.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry structure containing cached type information for the array type

## Dependencies
- Functions called/Symbols referenced:
  - [cache_array_element_properties](../c/cache_array_element_properties.md)
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag constant)
  - TCFLAGS_HAVE_ELEM_EXTENDED_HASHING (flag constant)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md) (at line 750)

## Notes and Other Information
This is a static function in typcache.c that serves as part of the type caching system for extended hash functionality. Extended hashing is particularly important for improved hash distribution and performance in large-scale operations. This functionality represents an enhancement over traditional 32-bit hashing and provides better hash collision resistance. The function implements the same lazy evaluation pattern as its companion array element property checking functions.
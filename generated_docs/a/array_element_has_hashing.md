# array_element_has_hashing

## Location
src/backend/utils/cache/typcache.c: 1443 - 1450

## Overview
Determines whether an array type has a hash function available for its element type.

## Definition
```c
static bool array_element_has_hashing(TypeCacheEntry *typentry)
```

## Detailed Description
This function checks if the array element type associated with a given type cache entry has hashing functionality available. It follows the same pattern as `array_element_has_compare`, first ensuring that element properties have been cached by checking the `TCFLAGS_CHECKED_ELEM_PROPERTIES` flag, and if not, it calls `cache_array_element_properties()` to populate the cache. Once the properties are cached, it returns whether the `TCFLAGS_HAVE_ELEM_HASHING` flag is set, indicating that hash operations are available for the array element type.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry structure containing cached type information for the array type

## Dependencies
- Functions called/Symbols referenced:
  - [cache_array_element_properties](../c/cache_array_element_properties.md)
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag constant)
  - TCFLAGS_HAVE_ELEM_HASHING (flag constant)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md) (at line 701)

## Notes and Other Information
This is a static function in typcache.c that serves as part of the type caching system for hash functionality. Like its comparison counterpart, it implements lazy evaluation of element properties. Hash functions are essential for operations like hash joins, hash aggregation, and hash-based data structures in PostgreSQL. The function is used by the type cache lookup functionality to determine if array element types support hashing operations.
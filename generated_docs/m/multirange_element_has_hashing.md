# multirange_element_has_hashing

## Location
src/backend/utils/cache/typcache.c: 1666 - 1673

## Overview
Checks whether the element type of a multirange type supports regular hashing (basic hash functions).

## Definition
```c
static bool multirange_element_has_hashing(TypeCacheEntry *typentry)
```

## Detailed Description
This function determines if the element type of a multirange type has basic hashing capabilities. Regular hashing refers to the standard hash functions used in PostgreSQL for basic hash operations. The function first ensures that element properties have been cached by checking the TCFLAGS_CHECKED_ELEM_PROPERTIES flag, and if not, it calls cache_multirange_element_properties() to populate the cache. It then returns whether the TCFLAGS_HAVE_ELEM_HASHING flag is set, indicating that the element type has a valid hash function.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry for the multirange type whose element hashing capability is being checked

## Dependencies
- Functions called/Symbols referenced:
  - [cache_multirange_element_properties](../c/cache_multirange_element_properties.md)
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag constant)
  - TCFLAGS_HAVE_ELEM_HASHING (flag constant)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md)

## Notes and Other Information
This is a static helper function within the type cache system, specifically for multirange types. Like its range counterpart, it follows a lazy evaluation pattern to avoid unnecessary computations. The function is parallel to range_element_has_hashing but operates on multirange types, which are collections of non-overlapping ranges. Regular hashing capability is essential for basic hash operations like hash tables and simple hash-based comparisons.
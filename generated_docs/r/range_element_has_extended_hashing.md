# range_element_has_extended_hashing

## Location
src/backend/utils/cache/typcache.c: 1634 - 1641

## Overview
Checks whether the element type of a range type supports extended hashing (hash functions for hash partitioning and hash joins).

## Definition


## Detailed Description
This function determines if the element type of a range type has extended hashing capabilities. Extended hashing refers to hash functions that support hash partitioning and hash joins in PostgreSQL. The function first ensures that element properties have been cached by checking the TCFLAGS_CHECKED_ELEM_PROPERTIES flag, and if not, it calls cache_range_element_properties() to populate the cache. It then returns whether the TCFLAGS_HAVE_ELEM_EXTENDED_HASHING flag is set.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry for the range type whose element extended hashing capability is being checked

## Dependencies
- Functions called/Symbols referenced:
  - [cache_range_element_properties](../c/cache_range_element_properties.md)
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag constant)
  - TCFLAGS_HAVE_ELEM_EXTENDED_HASHING (flag constant)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md)

## Notes and Other Information
This is a static helper function within the type cache system. The function follows a lazy evaluation pattern - it only computes and caches the element properties when they haven't been checked before. The extended hashing capability is important for performance optimizations in hash-based operations like hash joins and hash partitioning.
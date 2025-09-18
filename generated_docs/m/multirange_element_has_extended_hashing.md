# multirange_element_has_extended_hashing

## Location
[src/backend/utils/cache/typcache.c:1674-1681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1674-L1681)

## Overview
Checks whether the element type of a multirange type supports extended hashing (hash functions for hash partitioning and hash joins).

## Definition
```c
static bool multirange_element_has_extended_hashing(TypeCacheEntry *typentry)
```

## Detailed Description
This function determines if the element type of a multirange type has extended hashing capabilities. Extended hashing refers to hash functions that support hash partitioning and hash joins in PostgreSQL. The function first ensures that element properties have been cached by checking the TCFLAGS_CHECKED_ELEM_PROPERTIES flag, and if not, it calls cache_multirange_element_properties() to populate the cache. It then returns whether the TCFLAGS_HAVE_ELEM_EXTENDED_HASHING flag is set, indicating that the element type supports extended hash operations.

## Parameters / Member Variables
- `typentry`: A pointer to the TypeCacheEntry for the multirange type whose element extended hashing capability is being checked

## Dependencies
- Functions called/Symbols referenced:
  - [cache_multirange_element_properties](../c/cache_multirange_element_properties.md)
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag constant)
  - TCFLAGS_HAVE_ELEM_EXTENDED_HASHING (flag constant)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md)

## Notes and Other Information
This is a static helper function within the type cache system, specifically for multirange types. It mirrors the functionality of range_element_has_extended_hashing but operates on multirange types. The function follows the same lazy evaluation pattern to ensure efficient caching. Extended hashing capability is crucial for performance optimizations in advanced hash-based operations like hash joins and hash partitioning, particularly important for multirange types which can contain multiple range elements.
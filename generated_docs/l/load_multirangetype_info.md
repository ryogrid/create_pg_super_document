# load_multirangetype_info

## Location
[src/backend/utils/cache/typcache.c:972-993](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L972-L993)

## Overview
A helper function that sets up multirange type information in the PostgreSQL type cache system by loading the corresponding range type information.

## Definition

```c
static void
load_multirangetype_info(TypeCacheEntry *typentry)
```
## Detailed Description
This function is responsible for loading multirange type information into a type cache entry. It retrieves the underlying range type OID for a given multirange type and populates the cache entry with the corresponding range type information. The function is part of PostgreSQL's type caching mechanism that optimizes type-related operations by storing frequently accessed type information.

When called, the function:
1. Gets the range type OID that corresponds to the multirange type using 
2. Validates that the returned OID is valid
3. Looks up and caches the range type information using  with the  flag

## Parameters / Member Variables
- : A pointer to the TypeCacheEntry structure that will be populated with multirange type information. This entry represents the multirange type being processed.

## Dependencies
- Functions called/Symbols referenced:
  - [get_multirange_range](../g/get_multirange_range.md)
  - [lookup_type_cache](lookup_type_cache.md)
  - TYPECACHE_RANGE_INFO
- Called from (representative examples):
  - [lookup_type_cache](lookup_type_cache.md)
  - [cache_multirange_element_properties](../c/cache_multirange_element_properties.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same compilation unit (typcache.c)
- The function will throw an ERROR if the multirange type lookup fails, indicating a corrupt system catalog
- Part of PostgreSQL's type cache infrastructure that supports efficient multirange type operations
- Multirange types were introduced in PostgreSQL 14 as arrays of ranges with specific semantics
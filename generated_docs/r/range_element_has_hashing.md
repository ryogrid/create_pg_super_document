# range_element_has_hashing

## Location
src/backend/utils/cache/typcache.c: 1626 - 1633

## Overview
This function checks whether the element type of a range or multirange type supports hashing operations, enabling hash-based operations on range types.

## Definition
static bool range_element_has_hashing(TypeCacheEntry *typentry)

## Detailed Description
The function determines if a range or multirange type's element type supports hashing operations by checking cached properties. It reuses the array element property flag bits for range types since those flags are otherwise unused for range types. The function follows the standard lazy evaluation pattern, ensuring element properties are cached via cache_range_element_properties before returning the cached result. This enables PostgreSQL to determine whether range types can participate in hash-based operations like hash joins and hash aggregation.

## Parameters / Member Variables
- typentry: Pointer to a TypeCacheEntry structure containing cached information about a range or multirange type, including flags indicating which operations are supported by the element type

## Dependencies
- Functions called/Symbols referenced:
  - cache_range_element_properties
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag)
  - TCFLAGS_HAVE_ELEM_HASHING (flag)
- Called from (representative examples):
  - lookup_type_cache

## Notes and Other Information
- This is a static function only used within typcache.c
- Cleverly reuses array element property flags for range types since they would otherwise be unused
- Implements lazy evaluation pattern consistent with other type property checking functions
- The result is cached to avoid repeated computation of element properties
- Essential for enabling hash-based operations on range and multirange types
- Part of PostgreSQL's range type system that allows operations on ranges based on their element type capabilities
- Works in conjunction with similar functions for comparison operations on range types
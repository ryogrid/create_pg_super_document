# array_element_has_equality

## Location
[src/backend/utils/cache/typcache.c:1427-1434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/cache/typcache.c#L1427-L1434)

## Overview
A helper routine that checks whether array equality operations (like array_eq) should be expected to work on a given array type by verifying that the array element type supports equality.

## Definition

```c
static bool
array_element_has_equality(TypeCacheEntry *typentry)
```
## Detailed Description
This static function is part of a family of helper routines designed to determine the capabilities of array and composite types for various operations. It specifically checks whether the element type of an array supports equality operations, which is essential for determining if array comparison functions will work correctly.

The function uses a caching mechanism through the type cache entry flags to avoid repeated expensive lookups. If the element properties haven't been checked yet (indicated by the TCFLAGS_CHECKED_ELEM_PROPERTIES flag), it calls cache_array_element_properties to populate all element property information at once. This design assumes that if one property is needed, others will likely be needed as well, making the comprehensive caching approach more efficient.

## Parameters / Member Variables
- `*typentry`: Pointer to the TypeCacheEntry structure for the array type being checked
## Dependencies
- Functions called/Symbols referenced:
  - TCFLAGS_CHECKED_ELEM_PROPERTIES (flag indicating element properties have been cached)
  - [cache_array_element_properties](../c/cache_array_element_properties.md) (function that caches all element properties)
  - TCFLAGS_HAVE_ELEM_EQUALITY (flag indicating element type supports equality)
- Called from (representative examples):
  - [lookup_type_cache](../l/lookup_type_cache.md) (type cache lookup operations)

## Notes and Other Information
- This is a static function internal to the typcache.c module
- The function is part of a family of similar helper routines for checking array/composite type capabilities
- Caching is used to optimize performance since these checks may be called repeatedly on the same types
- The comprehensive property caching approach assumes that multiple properties will typically be needed together
- The function specifically supports the functionality of array_eq and related array comparison operations

## Simplified Source

```c
static bool
array_element_has_equality(TypeCacheEntry *typentry)
{
    // Ensure element properties are cached
    if (!(typentry->flags & TCFLAGS_CHECKED_ELEM_PROPERTIES))
        cache_array_element_properties(typentry);

    // Return whether element type has equality support
    return (typentry->flags & TCFLAGS_HAVE_ELEM_EQUALITY) != 0;
}
```
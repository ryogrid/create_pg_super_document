# multirange_bsearch_match

## Location
[src/backend/utils/adt/multirangetypes.c:898-940](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L898-L940)

## Overview
Performs binary search within a multirange to find if any range matches a given key using a custom comparison function.

## Definition

```c
static bool
multirange_bsearch_match(TypeCacheEntry *typcache, const MultirangeType *mr,
						 void *key, multirange_bsearch_comparison cmp_func)
```
## Detailed Description
This function implements a generic binary search algorithm for multirange types that can be customized with different comparison functions. It searches through the sorted ranges within a multirange to find one that matches the given key according to the provided comparison function. The comparison function determines both the search direction and whether a found range constitutes a match. This design allows the same binary search logic to be used for different operations like containment checks, overlap detection, and exact matching.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing metadata and functions for the range element type
- `*mr`: The multirange to search within
- `*key`: Pointer to the search key (type depends on the specific operation)
- `cmp_func`: Comparison function that guides the search and determines matches
## Dependencies
- Functions called/Symbols referenced:
  - [multirange_get_bounds](multirange_get_bounds.md) (extracts range boundaries for comparison)
  - MultirangeType (multirange structure type)
  - RangeBound (boundary structure type)
- Called from (representative examples):
  - [multirange_contains_elem_internal](multirange_contains_elem_internal.md)
  - [multirange_contains_range_internal](multirange_contains_range_internal.md)
  - [range_overlaps_multirange_internal](../r/range_overlaps_multirange_internal.md)

## Notes and Other Information
- This is a static function, internal to the multirange implementation
- Uses standard binary search algorithm with O(log n) complexity
- The comparison function can report both search direction and match status
- Supports various operations through different comparison functions
- Returns false if no range is found or if the comparison function reports no match
- Critical optimization for multirange operations that need to locate specific ranges
- Located in src/backend/utils/adt/multirangetypes.c

## Simplified Source

```c
static bool
multirange_bsearch_match(TypeCacheEntry *typcache, const MultirangeType *mr,
                        void *key, multirange_bsearch_comparison cmp_func)
{
    uint32 left = 0;
    uint32 right = mr->rangeCount;
    bool match = false;

    // Binary search through sorted ranges
    while (left < right) {
        uint32 middle = (left + right) / 2;
        RangeBound lower, upper;

        // Get bounds of current range
        multirange_get_bounds(typcache, mr, middle, &lower, &upper);

        // Compare using custom comparison function
        int comparison = (*cmp_func)(typcache, &lower, &upper, key, &match);

        if (comparison < 0)
            right = middle;           // Search left half
        else if (comparison > 0)
            left = middle + 1;        // Search right half
        else
            return match;             // Found range, return match status
    }

    return false;  // No matching range found
}
```

This function performs a binary search on a multirange's sorted ranges using a custom comparison function to determine both search direction and match criteria.
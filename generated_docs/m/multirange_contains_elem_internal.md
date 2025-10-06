# multirange_contains_elem_internal

## Location
[src/backend/utils/adt/multirangetypes.c:1707-1720](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1707-L1720)

## Overview
Tests whether a multirange contains a specific element value by performing a binary search through the multirange's constituent ranges.

## Definition
```c
bool multirange_contains_elem_internal(TypeCacheEntry *rangetyp,
                                      const MultirangeType *mr, Datum val)
```

## Detailed Description
This function determines if a given element value is contained within any of the ranges that comprise a multirange. It first checks if the multirange is empty (in which case it returns false immediately), then uses binary search with a specialized comparison function to efficiently locate whether the element falls within any of the multirange's constituent ranges.

The function leverages the sorted nature of ranges within a multirange to perform an efficient O(log n) search rather than a linear scan through all ranges.

## Parameters / Member Variables
- `rangetyp`: Type cache entry for the range type, containing comparison functions and type information
- `mr`: Pointer to the multirange to search within
- `val`: The element value (as a Datum) to search for

## Dependencies
- Functions called/Symbols referenced:
  - MultirangeType (struct type)
  - MultirangeIsEmpty (utility function)
  - [multirange_bsearch_match](multirange_bsearch_match.md) (binary search function)
  - [multirange_elem_bsearch_comparison](multirange_elem_bsearch_comparison.md) (comparison callback)
- Called from (representative examples):
  - [multirange_contains_elem](multirange_contains_elem.md)
  - [elem_contained_by_multirange](../e/elem_contained_by_multirange.md)
  - PG_RETURN_MULTIRANGE_P (via macro expansion)

## Notes and Other Information
This is an internal utility function that provides the core logic for element containment testing. It's designed to be called by the public PostgreSQL functions that implement the @> (contains) and <@ (contained by) operators for multiranges and elements. The function assumes the multirange is well-formed with non-overlapping, sorted ranges.

## Simplified Source

```c
bool multirange_contains_elem_internal(TypeCacheEntry *rangetyp,
                                      const MultirangeType *multirange, Datum element_value) {
    // Empty multiranges contain no elements
    if (MultirangeIsEmpty(multirange))
        return false;

    // Use binary search to efficiently find if element is contained in any range
    return multirange_bsearch_match(rangetyp, multirange, &element_value,
                                   multirange_elem_bsearch_comparison);
}
```
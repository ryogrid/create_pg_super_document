# range_overleft_multirange_internal

## Location
src/backend/utils/adt/multirangetypes.c: 2073 - 2095

## Overview
Internal function that checks if a range does not extend to the right of a multirange (i.e., range is "overleft" of multirange).

## Definition
```c
bool range_overleft_multirange_internal(TypeCacheEntry *rangetyp,
                                       const RangeType *r,
                                       const MultirangeType *mr)
```

## Detailed Description
This function implements the "overleft" or "does not extend to right of" operation between a range and a multirange. The overleft relationship means that the range's upper bound is less than or equal to the multirange's upper bound.

The function works by comparing the upper bound of the input range `r` with the upper bound of the rightmost (last) range in the multirange `mr`. Since ranges within a multirange are stored in sorted order, the last range has the highest upper bound, making this an efficient O(1) operation.

The function handles empty inputs by returning false, following PostgreSQL's range semantics where empty ranges have no meaningful positional relationships.

## Parameters / Member Variables
- `rangetyp`: Type cache entry for the range type, providing comparison functions and type metadata
- `r`: The range to check for overleft relationship
- `mr`: The multirange to compare against

## Dependencies
- Functions called/Symbols referenced:
  - `RangeIsEmpty` - Check if the input range is empty
  - `MultirangeIsEmpty` - Check if the input multirange is empty
  - `[range_deserialize](range_deserialize.md)` - Extract bounds from the range structure
  - `[multirange_get_bounds](../m/multirange_get_bounds.md)` - Get bounds of the last range in the multirange (using `mr->rangeCount - 1`)
  - `[range_cmp_bounds](range_cmp_bounds.md)` - Compare the upper bounds to determine overleft relationship
  - `PG_RETURN_BOOL` - Return the boolean result
  - `RangeBound` - Structure for range boundary representation
- Called from (representative examples):
  - SQL queries using the `&<` operator between range and multirange types
  - PostgreSQL's operator system for range comparisons

## Notes and Other Information
- Returns false immediately if either input is empty
- Uses `mr->rangeCount - 1` to access the rightmost range in the multirange for comparison
- The comparison `range_cmp_bounds(rangetyp, &upper1, &upper2) <= 0` implements the overleft logic
- Located in `src/backend/utils/adt/multirangetypes.c` at lines 2073-2095
- Part of PostgreSQL's comprehensive set of spatial relationship operators for range and multirange types
- The comment "does not extend to right of?" clearly indicates the semantic meaning of this operation
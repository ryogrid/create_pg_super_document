# range_split_internal

## Location
[src/backend/utils/adt/rangetypes.c:1182-1218](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L1182-L1218)

## Overview
Splits a range by removing an intersecting range from its middle, returning two output ranges if the split creates non-empty ranges on both sides.

## Definition

```c
bool
range_split_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2,
					 RangeType **output1, RangeType **output2)
```
## Detailed Description
The  function performs a specialized range subtraction operation where range  is subtracted from range , but only if  intersects the middle portion of , leaving non-empty ranges on both sides. This is essentially computing  when the result would be two disjoint ranges.

The function first deserializes both input ranges to access their boundary information. It then checks if  completely fits within  (i.e.,  and ). If this condition is met, the function creates two output ranges: one from 's lower bound to 's lower bound (with inverted inclusivity), and another from 's upper bound (with inverted inclusivity) to 's upper bound.

The inclusivity inversion is crucial for proper range semantics - when splitting at a boundary, the boundary point should not be included in both resulting ranges.

## Parameters / Member Variables
- `*typcache`: Type cache entry containing comparison functions and metadata for the range type
- `*r1`: The range to be split (const RangeType pointer)
- `*r2`: The range to subtract from r1 (const RangeType pointer)
- `**output1`: Pointer to store the first resulting range (the left portion)
- `**output2`: Pointer to store the second resulting range (the right portion)
## Dependencies
- Functions called/Symbols referenced:
  - : Extracts boundary information from range values
  - : Compares range boundaries using type-specific comparison
  - : Constructs new range values from boundary specifications
  - : Structure representing range boundary information
- Called from:
  - : Multirange subtraction operations
  - : Range strategy function for equality operations

## Notes and Other Information
- Returns true if the split was successful (two non-empty ranges produced), false otherwise
- Neither input range should be empty (this is a precondition documented in the function comment)
- The function only splits when r2 intersects the middle of r1, not when it overlaps at the edges
- Boundary inclusivity is carefully inverted to maintain proper range semantics in the output ranges
- Output parameters are only set when the function returns true
- Located in
- This function is primarily used for implementing range difference operations that result in multiple disjoint ranges

## Simplified Source

```c
bool range_split_internal(TypeCacheEntry *typcache, const RangeType *r1, const RangeType *r2,
                         RangeType **output1, RangeType **output2) {
    RangeBound lower1, lower2, upper1, upper2;
    bool empty1, empty2;

    // Extract boundaries from both ranges
    range_deserialize(typcache, r1, &lower1, &upper1, &empty1);
    range_deserialize(typcache, r2, &lower2, &upper2, &empty2);

    // Check if r2 is completely contained within r1 (split condition)
    if (range_cmp_bounds(typcache, &lower1, &lower2) < 0 &&
        range_cmp_bounds(typcache, &upper1, &upper2) > 0) {

        // Invert inclusivity for boundary points to avoid overlap
        lower2.inclusive = !lower2.inclusive;
        lower2.lower = false;  // Will become upper bound of first range
        upper2.inclusive = !upper2.inclusive;
        upper2.lower = true;   // Will become lower bound of second range

        // Create the two output ranges: [lower1, lower2) and (upper2, upper1]
        *output1 = make_range(typcache, &lower1, &lower2, false, NULL);
        *output2 = make_range(typcache, &upper2, &upper1, false, NULL);
        return true;
    }

    return false;
}
```
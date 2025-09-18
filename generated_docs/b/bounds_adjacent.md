# bounds_adjacent

## Location
src/backend/utils/adt/rangetypes.c: 757 - 797

## Overview
Determines if two range bounds are "adjacent" where one is an upper bound and the other is a lower bound, with no values existing between them.

## Definition


## Detailed Description
This function checks if two range bounds are adjacent, meaning that no subtype values exist that satisfy neither bound (gap between bounds) or both bounds (overlap). The function expects boundA to be an upper bound and boundB to be a lower bound.

The adjacency check works differently based on the range subtype:
- For discrete ranges: Uses the canonicalization function to create a test range A..B and checks if it normalizes to empty
- For continuous ranges: Without a canonicalization function, bounds separated by any gap are not considered adjacent
- For equal bounds: Adjacent only if they have different inclusivity flags (exactly one includes the boundary point)

## Parameters / Member Variables
- : TypeCacheEntry containing range type information and canonicalization function
- : RangeBound representing an upper bound to test
- : RangeBound representing a lower bound to test

## Dependencies
- Functions called/Symbols referenced:
  - [range_cmp_bound_values](../r/range_cmp_bound_values.md)
  - [make_range](../m/make_range.md)
  - RangeIsEmpty
  - OidIsValid (macro)
- Called from (representative examples):
  - [range_adjacent_internal](../r/range_adjacent_internal.md)
  - [range_adjacent_multirange_internal](../r/range_adjacent_multirange_internal.md)
  - [multirange_adjacent_multirange](../m/multirange_adjacent_multirange.md)
  - [adjacent_cmp_bounds](../a/adjacent_cmp_bounds.md)

## Notes and Other Information
- Asserts that boundA is not a lower bound and boundB is a lower bound
- For continuous subtypes without canonicalization, assumes there are always points between non-equal bounds
- The function temporarily flips inclusion flags and bound types when testing discrete ranges
- Critical for implementing PostgreSQL's range adjacency operators (-|-, |-)
- Returns false if bounds overlap (cmp > 0)
- Located in src/backend/utils/adt/rangetypes.c:757-797
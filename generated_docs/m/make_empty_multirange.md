# make_empty_multirange

## Location
src/backend/utils/adt/multirangetypes.c: 848 - 857

## Overview
Creates an empty multirange of the specified multirange type, containing no ranges.

## Definition


## Detailed Description
This function constructs an empty multirange object by calling the general  function with zero ranges. An empty multirange represents a collection that contains no range values, which is a valid state in PostgreSQL's multirange type system. This is commonly used as a starting point for multirange operations or as a result when operations produce no overlapping ranges.

## Parameters / Member Variables
- : Object identifier for the multirange type to create
- : Type cache entry containing metadata about the underlying range type

## Dependencies
- Functions called/Symbols referenced:
  - [make_multirange](make_multirange.md)
- Called from (representative examples):
  - [multirange_intersect](multirange_intersect.md)
  - PG_RETURN_MULTIRANGE_P (macro usage)

## Notes and Other Information
- This is a convenience function that simplifies the creation of empty multiranges
- The function delegates to  with 0 ranges and NULL range array
- Empty multiranges are fundamental in multirange algebra, serving as the identity element for union operations
- Located in 
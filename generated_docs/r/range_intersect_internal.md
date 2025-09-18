# range_intersect_internal

## Location
src/backend/utils/adt/rangetypes.c: 1143 - 1181

## Overview
Performs the core intersection logic for two range values, computing the overlapping portion and returning a new range containing only the intersection.

## Definition


## Detailed Description
The  function implements the actual intersection algorithm for PostgreSQL range types. It deserializes both input ranges to extract their boundary information, determines if they overlap, and if so, constructs a new range representing their intersection. The intersection is computed by taking the maximum of the lower bounds and the minimum of the upper bounds of the two input ranges.

The function handles several edge cases: if either input range is empty or if the ranges do not overlap, it returns an empty range. Otherwise, it carefully compares the boundaries using the appropriate comparison function for the range's element type to determine the correct intersection boundaries.

## Parameters / Member Variables
- : Type cache entry containing comparison functions and other metadata for the range type
- : First input range (const RangeType pointer)
- : Second input range (const RangeType pointer)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts boundary and empty flag information from range values
  - : Checks if two ranges have any overlap
  - : Creates an empty range of the specified type
  - : Compares range boundaries using type-specific comparison
  - : Constructs a new range from boundary specifications
  - : Structure representing range boundary information
- Called from:
  - : SQL-callable wrapper function
  - : Aggregate function transition function
  - : Multirange intersection implementation
  - : Range strategy function for equality operations

## Notes and Other Information
- Returns an empty range if either input is empty or if ranges do not overlap
- Uses type-specific comparison functions through the typcache for proper boundary ordering
- The intersection algorithm: lower bound = max(lower1, lower2), upper bound = min(upper1, upper2)
- This is a core utility function used by multiple higher-level range operations
- Located in 
- The function is designed to be reusable across different range operation contexts
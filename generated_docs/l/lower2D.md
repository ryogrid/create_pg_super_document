# lower2D

## Location
[src/backend/utils/adt/geo_spgist.c:286-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L286-L293)

## Overview
The `lower2D` function determines whether any range from a `RangeBox` can be positioned lower than a given `Range` query in 2-dimensional space.

## Definition
```c
static bool lower2D(RangeBox *range_box, Range *query)
```

## Detailed Description
This function is part of PostgreSQL's geometric SP-GiST implementation for 2D spatial operations. It evaluates whether any range within the provided `RangeBox` structure has coordinate values that are positioned "lower" than the specified `Range` query. The function performs this check by comparing the low bounds of both the left and right ranges within the `RangeBox` against the low bound of the query range using floating-point less-than comparisons. The function returns true only if both dimensional ranges have lower bounds that are strictly less than the query's lower bound.

## Parameters / Member Variables
- `range_box`: A pointer to a `RangeBox` structure containing left and right range boundaries to be tested
- `query`: A pointer to a `Range` structure representing the reference range for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [FPlt](../F/FPlt.md) (floating-point less-than comparison function, called twice)
- Data types used:
  - [RangeBox](../R/RangeBox.md)
  - [Range](../R/Range.md)
- Called from (representative examples):
  - [left4D](left4D.md)
  - [below4D](../b/below4D.md)

## Notes and Other Information
- This is a static function with internal linkage within the geo_spgist.c file
- The function is used in SP-GiST index operations for spatial positioning queries
- It uses the `FPlt` function for robust floating-point comparisons to avoid precision issues
- The function implements a "strictly lower" comparison, requiring both left and right range lower bounds to be less than the query's lower bound
- This function is utilized by higher-level 4D positioning functions like `left4D` and `below4D`
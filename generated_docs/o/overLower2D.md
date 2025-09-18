# overLower2D

## Location
src/backend/utils/adt/geo_spgist.c: 294 - 301

## Overview
The `overLower2D` function determines whether any range from a `RangeBox` does not extend beyond the right side of a given `Range` query in 2-dimensional space.

## Definition
```c
static bool overLower2D(RangeBox *range_box, Range *query)
```

## Detailed Description
This function is part of PostgreSQL's geometric SP-GiST implementation for 2D spatial operations. It evaluates whether any range within the provided `RangeBox` structure has lower bounds that do not exceed the upper bound of the specified `Range` query. The function performs this check by comparing the low bounds of both the left and right ranges within the `RangeBox` against the high bound of the query range using floating-point less-than-or-equal comparisons. This effectively determines if the ranges can overlap or be positioned to the left of the query's right boundary, rather than being completely to the right of it.

## Parameters / Member Variables
- `range_box`: A pointer to a `RangeBox` structure containing left and right range boundaries to be tested
- `query`: A pointer to a `Range` structure representing the reference range for boundary comparison

## Dependencies
- Functions called/Symbols referenced:
  - FPle (floating-point less-than-or-equal comparison function, called twice)
- Data types used:
  - RangeBox
  - Range
- Called from (representative examples):
  - overLeft4D
  - overBelow4D

## Notes and Other Information
- This is a static function with internal linkage within the geo_spgist.c file
- The function is used in SP-GiST index operations for spatial overlap and positioning queries
- It uses the `FPle` function for robust floating-point comparisons to handle precision issues
- The function implements an "overlap or left-of" check, ensuring that range lower bounds are at most equal to the query's upper bound
- This function is utilized by higher-level 4D overlap functions like `overLeft4D` and `overBelow4D`
- The comment indicates it checks if ranges "not extend to the right side" of the query, meaning they either overlap or are positioned to the left
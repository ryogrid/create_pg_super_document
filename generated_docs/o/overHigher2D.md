# overHigher2D

## Location
[src/backend/utils/adt/geo_spgist.c:310-317](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L310-L317)

## Overview
The `overHigher2D` function determines whether any range from a `RangeBox` does not extend beyond the left side of a given `Range` query in 2-dimensional space.

## Definition
```c
static bool overHigher2D(RangeBox *range_box, Range *query)
```

## Detailed Description
This function is part of PostgreSQL's geometric SP-GiST implementation for 2D spatial operations. It evaluates whether any range within the provided `RangeBox` structure has upper bounds that do not fall below the lower bound of the specified `Range` query. The function performs this check by comparing the high bounds of both the left and right ranges within the `RangeBox` against the low bound of the query range using floating-point greater-than-or-equal comparisons. This effectively determines if the ranges can overlap or be positioned to the right of the query's left boundary, rather than being completely to the left of it.

## Parameters / Member Variables
- `range_box`: A pointer to a `RangeBox` structure containing left and right range boundaries to be tested
- `query`: A pointer to a `Range` structure representing the reference range for boundary comparison

## Dependencies
- Functions called/Symbols referenced:
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison function, called twice)
- Data types used:
  - [RangeBox](../R/RangeBox.md)
  - [Range](../R/Range.md)
- Called from (representative examples):
  - [overRight4D](overRight4D.md)
  - [overAbove4D](overAbove4D.md)

## Notes and Other Information
- This is a static function with internal linkage within the geo_spgist.c file
- The function is used in SP-GiST index operations for spatial overlap and positioning queries
- It uses the `FPge` function for robust floating-point comparisons to handle precision issues
- The function implements an "overlap or right-of" check, ensuring that range upper bounds are at least equal to the query's lower bound
- This function is the complement to `overLower2D`, dealing with upper bounds against lower query bounds instead of lower bounds against upper query bounds
- This function is utilized by higher-level 4D overlap functions like `overRight4D` and `overAbove4D`
- The comment indicates it checks if ranges "not extend to the left side" of the query, meaning they either overlap or are positioned to the right
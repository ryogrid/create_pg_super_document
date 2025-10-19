# overLower2D

## Location
[src/backend/utils/adt/geo_spgist.c:294-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L294-L301)

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
  - [FPle](../F/FPle.md) (floating-point less-than-or-equal comparison function, called twice)
- Data types used:
  - [RangeBox](../R/RangeBox.md)
  - [Range](../R/Range.md)
- Called from (representative examples):
  - [overLeft4D](overLeft4D.md)
  - [overBelow4D](overBelow4D.md)

## Notes and Other Information
- This is a static function with internal linkage within the geo_spgist.c file
- The function is used in SP-GiST index operations for spatial overlap and positioning queries
- It uses the `FPle` function for robust floating-point comparisons to handle precision issues
- The function implements an "overlap or left-of" check, ensuring that range lower bounds are at most equal to the query's upper bound
- This function is utilized by higher-level 4D overlap functions like `overLeft4D` and `overBelow4D`
- The comment indicates it checks if ranges "not extend to the right side" of the query, meaning they either overlap or are positioned to the left

## Simplified Source

```c
/* Check if any range from range_box doesn't extend past query's right side */
static bool
overLower2D(RangeBox *range_box, Range *query)
{
    // Both left and right ranges must start at or before query's end
    bool left_not_past = (range_box->left.low <= query->high);
    bool right_not_past = (range_box->right.low <= query->high);

    return left_not_past && right_not_past;
}
```
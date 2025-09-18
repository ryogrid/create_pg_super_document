# contained2D

## Location
src/backend/utils/adt/geo_spgist.c: 268 - 277

## Overview
The `contained2D` function determines whether any range from a range box can be contained by (i.e., fit within) a given query range in 2D space.

## Definition
```c
static bool contained2D(RangeBox *range_box, Range *query)
```

## Detailed Description
This static helper function tests for the reverse containment relationship in 2D space within PostgreSQL spatial indexing (SP-GiST). Unlike `contain2D` which tests if the range box can contain the query, this function tests if any part of the range box can be contained by the query range. The function performs four floating-point comparisons to ensure that both the left and right ranges of the range box have some overlap with the query range, meaning they can potentially be contained by it.

## Parameters / Member Variables
- `range_box`: Pointer to a RangeBox structure containing left and right Range objects that might be contained
- `query`: Pointer to a Range structure representing the query range that serves as the potential container

## Dependencies
- Functions called/Symbols referenced:
  - [FPle](../F/FPle.md) (floating-point less-than-or-equal comparison, used twice)
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison, used twice)
  - Range (structure type)
  - RangeBox (structure type)
- Called from (representative examples):
  - [contained4D](contained4D.md)

## Notes and Other Information
- This function is part of the SP-GiST implementation for geometric data types
- Implements the inverse containment logic compared to `contain2D`
- Uses four separate floating-point comparisons to check if either the left or right range can fit within the query bounds
- The logic checks: left.low ≤ query.high, left.high ≥ query.low, right.low ≤ query.high, right.high ≥ query.low
- Returns true if there is potential for containment, false otherwise
- Serves as a building block for higher-dimensional contained tests like `contained4D`
- Critical for implementing "contained by" spatial operators in PostgreSQL geometric queries
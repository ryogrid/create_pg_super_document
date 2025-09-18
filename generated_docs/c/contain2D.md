# contain2D

## Location
src/backend/utils/adt/geo_spgist.c: 252 - 259

## Overview
The `contain2D` function determines whether any range from a range box can completely contain a given query range in 2D space.

## Definition
```c
static bool contain2D(RangeBox *range_box, Range *query)
```

## Detailed Description
This static helper function tests for containment relationship in 2D space within PostgreSQL spatial indexing (SP-GiST). The function checks if the range box can completely contain the query range by verifying that the range box boundaries extend beyond or equal to the query boundaries on both sides. It ensures the right boundary of the range box is greater than or equal to the high value of the query, and the left boundary is less than or equal to the low value of the query.

## Parameters / Member Variables
- `range_box`: Pointer to a RangeBox structure containing left and right Range objects representing the container bounds
- `query`: Pointer to a Range structure representing the query range to test for containment

## Dependencies
- Functions called/Symbols referenced:
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison)
  - [FPle](../F/FPle.md) (floating-point less-than-or-equal comparison)
  - Range (structure type)
  - RangeBox (structure type)
- Called from (representative examples):
  - [contain4D](contain4D.md)

## Notes and Other Information
- This function is part of the SP-GiST implementation for geometric data types
- Implements strict containment logic: the range box must fully encompass the query range
- Uses floating-point comparison functions for robust numeric operations
- Returns true if containment exists, false otherwise
- Serves as a building block for higher-dimensional containment tests like contain4D
- The containment test is the logical AND of two boundary conditions
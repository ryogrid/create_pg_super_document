# overlap2D

## Location
src/backend/utils/adt/geo_spgist.c: 236 - 243

## Overview
The `overlap2D` function determines whether any range from a range box can overlap with a given query range in 2D space.

## Definition
```c
static bool overlap2D(RangeBox *range_box, Range *query)
```

## Detailed Description
This is a static helper function used in PostgreSQL spatial indexing (SP-GiST) for geometric operations. The function performs a 2D overlap test by checking if there is any intersection between a range box and a query range. It uses floating-point comparison functions to determine if the ranges overlap by comparing the high value of the right range with the low value of the query, and the low value of the left range with the high value of the query.

## Parameters / Member Variables
- `range_box`: Pointer to a RangeBox structure containing left and right Range objects representing the bounds
- `query`: Pointer to a Range structure representing the query range to test for overlap

## Dependencies
- Functions called/Symbols referenced:
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison)
  - [FPle](../F/FPle.md) (floating-point less-than-or-equal comparison)
  - Range (structure type)
  - RangeBox (structure type)
- Called from (representative examples):
  - [overlap4D](overlap4D.md)

## Notes and Other Information
- This function is part of the SP-GiST (Space-partitioned Generalized Search Tree) implementation for geometric data types
- Uses floating-point comparison functions (FPge, FPle) for robust numeric comparisons
- Returns true if any overlap exists, false otherwise
- Serves as a building block for higher-dimensional overlap tests like overlap4D
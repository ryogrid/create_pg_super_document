# above4D

## Location
src/backend/utils/adt/geo_spgist.c: 360 - 366

## Overview
A static helper function that determines if any rectangle from a given RectBox can be positioned above a specified query range.

## Definition
```c
static bool above4D(RectBox *rect_box, RangeBox *query)
```

## Detailed Description
The `above4D` function is part of PostgreSQL's SP-GiST (Space-Partitioned Generalized Search Tree) implementation for geometric data types. This function performs a 4D geometric comparison to check whether any rectangle within the provided `rect_box` parameter can be positioned above the boundaries defined by the `query` parameter. It accomplishes this by delegating the actual comparison to the `higher2D` function, specifically comparing the Y-axis range of the rectangle box with the right boundary of the query range.

This function is used in spatial indexing operations to optimize geometric queries by quickly determining spatial relationships between rectangles and query boundaries.

## Parameters / Member Variables
- `rect_box`: A pointer to a RectBox structure containing the rectangle box to be tested
- `query`: A pointer to a RangeBox structure representing the query boundaries for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [higher2D](../h/higher2D.md)
  - RectBox (type)
  - RangeBox (type)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geo_spgist.c file
- The function name suggests it operates in 4D space, but the implementation delegates to a 2D comparison function
- It's part of the SP-GiST indexing infrastructure for efficient spatial queries in PostgreSQL
- The function returns a boolean value indicating whether any rectangle from the rect_box can be above the query range
- This function is complementary to `overBelow4D`, handling the opposite spatial relationship
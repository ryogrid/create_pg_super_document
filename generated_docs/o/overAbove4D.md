# overAbove4D

## Location
[src/backend/utils/adt/geo_spgist.c:367-373](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L367-L373)

## Overview
A static helper function that determines if any rectangle from a given RectBox does not extend below a specified query range.

## Definition
```c
static bool overAbove4D(RectBox *rect_box, RangeBox *query)
```

## Detailed Description
The `overAbove4D` function is part of PostgreSQL's SP-GiST (Space-Partitioned Generalized Search Tree) implementation for geometric data types. This function performs a 4D geometric comparison to check whether any rectangle within the provided `rect_box` parameter does not extend below the boundaries defined by the `query` parameter. It accomplishes this by delegating the actual comparison to the `overHigher2D` function, specifically comparing the Y-axis range of the rectangle box with the right boundary of the query range.

This function is used in spatial indexing operations to optimize geometric queries by quickly eliminating rectangles that don't satisfy certain spatial relationships, specifically those that would extend below the query range.

## Parameters / Member Variables
- `rect_box`: A pointer to a RectBox structure containing the rectangle box to be tested
- `query`: A pointer to a RangeBox structure representing the query boundaries for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [overHigher2D](overHigher2D.md)
  - RectBox (type)
  - [RangeBox](../R/RangeBox.md) (type)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the geo_spgist.c file
- The function name suggests it operates in 4D space, but the implementation delegates to a 2D comparison function
- It's part of the SP-GiST indexing infrastructure for efficient spatial queries in PostgreSQL
- The function returns a boolean value indicating whether the spatial relationship condition is met
- This function is complementary to `overBelow4D`, handling rectangles that don't extend below rather than above the query range
- Works in conjunction with other 4D spatial comparison functions to provide comprehensive geometric indexing capabilities
# overlap4D

## Location
[src/backend/utils/adt/geo_spgist.c:244-251](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L244-L251)

## Overview
The `overlap4D` function determines whether any rectangle from a rectangle box can overlap with a query argument in 4D space by performing 2D overlap tests on both X and Y dimensions.

## Definition
```c
static bool overlap4D(RectBox *rect_box, RangeBox *query)
```

## Detailed Description
This static helper function extends the 2D overlap test to 4D space for rectangular geometric objects. It performs overlap detection by calling `overlap2D` twice: once for the X-dimension range and once for the Y-dimension range. The function returns true only if both dimensions show overlap, implementing the logical AND condition for 4D overlap detection in PostgreSQL spatial indexing.

## Parameters / Member Variables
- `rect_box`: Pointer to a RectBox structure containing range boxes for X and Y dimensions
- `query`: Pointer to a RangeBox structure representing the query bounds to test for overlap

## Dependencies
- Functions called/Symbols referenced:
  - [overlap2D](overlap2D.md) (called twice for X and Y dimensions)
  - RectBox (structure type)
  - RangeBox (structure type)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- This function is part of the SP-GiST implementation for geometric data types, specifically for rectangle operations
- Builds upon the 2D overlap function to provide 4D (2D rectangle) overlap testing
- Uses logical AND to ensure overlap exists in both X and Y dimensions
- Primarily used in spatial index consistency checking during query processing
- The function accesses range_box_x and range_box_y members from the RectBox, and left/right members from the query RangeBox
# contained4D

## Location
[src/backend/utils/adt/geo_spgist.c:278-285](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L278-L285)

## Overview
The `contained4D` function determines whether any rectangle from a `RectBox` can be contained by a given `RangeBox` query in 4-dimensional space (representing 2D box coordinates with x and y ranges).

## Definition
```c
static bool contained4D(RectBox *rect_box, RangeBox *query)
```

## Detailed Description
This function is part of PostgreSQL's geometric SP-GiST (Space-Partitioned Generalized Search Tree) implementation for handling 2D box operations. It evaluates whether any rectangle from the provided `RectBox` structure can be contained within the bounds specified by a `RangeBox` query. The function operates by checking containment in both X and Y dimensions separately using the `contained2D` helper function, effectively handling 4D coordinate space (x1, y1, x2, y2) that represents rectangular boundaries.

## Parameters / Member Variables
- `rect_box`: A pointer to a `RectBox` structure containing rectangle coordinate ranges to be tested for containment
- `query`: A pointer to a `RangeBox` structure representing the query bounds that may contain the rectangles

## Dependencies
- Functions called/Symbols referenced:
  - [contained2D](contained2D.md) (called twice, once for X dimension and once for Y dimension)
- Data types used:
  - RectBox
  - [RangeBox](../R/RangeBox.md)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- This is a static function, meaning it has internal linkage within the geo_spgist.c file
- The function is used in SP-GiST index operations for geometric box containment queries
- It leverages the `contained2D` function to handle each dimensional axis independently
- The function returns true only if both X and Y dimensional containment conditions are satisfied
# below4D

## Location
[src/backend/utils/adt/geo_spgist.c:346-352](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L346-L352)

## Overview
Determines if any rectangle from a RectBox can be positioned below a given query boundary.

## Definition

```c
static bool
below4D(RectBox *rect_box, RangeBox *query)
```
## Detailed Description
This function is part of PostgreSQL's SP-GiST implementation for geometric box operations. It evaluates whether any rectangle within the provided RectBox structure could potentially be positioned entirely below the specified query boundary. Unlike the other 4D functions that work with x-axis relationships, this function operates on the y-axis by comparing the rectangle box's y-range (range_box_y) with the query's right boundary using the lower2D helper function.

## Parameters / Member Variables
- `rect_box`: Pointer to RectBox structure containing the spatial boundaries to be evaluated
- `query`: Pointer to RangeBox structure defining the query boundary conditions

## Dependencies
- Functions called/Symbols referenced:
  - [lower2D](../l/lower2D.md)
  - RectBox (struct)
  - [RangeBox](../R/RangeBox.md) (struct)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
This function is used in SP-GiST index operations for spatial queries involving box geometries. It provides vertical positioning evaluation, complementing the horizontal positioning functions (left4D, right4D, overLeft4D, overRight4D). The function checks the y-axis relationship and is part of the comprehensive spatial relationship predicate system that enables efficient geometric indexing and query processing for PostgreSQL's spatial data types. Note that it compares the y-range with the query's right boundary, which may seem counterintuitive but follows the internal structure design of the RangeBox.
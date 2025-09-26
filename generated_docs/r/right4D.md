# right4D

## Location
[src/backend/utils/adt/geo_spgist.c:332-338](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L332-L338)

## Overview
Determines if any rectangle from a RectBox can be positioned to the right of a given query boundary.

## Definition

```c
static bool
right4D(RectBox *rect_box, RangeBox *query)
```
## Detailed Description
This function is part of PostgreSQL's SP-GiST implementation for geometric box operations. It evaluates whether any rectangle within the provided RectBox structure could potentially be positioned entirely to the right of the specified query boundary. The function operates by comparing the x-axis range of the rectangle box with the left boundary of the query range using the higher2D helper function, which checks if the rectangle's x-range can be higher than the query's left boundary.

## Parameters / Member Variables
- `rect_box`: Pointer to RectBox structure containing the spatial boundaries to be evaluated
- `query`: Pointer to RangeBox structure defining the query boundary conditions

## Dependencies
- Functions called/Symbols referenced:
  - [higher2D](../h/higher2D.md)
  - RectBox (struct)
  - [RangeBox](../R/RangeBox.md) (struct)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
This function is used in SP-GiST index operations for spatial queries involving box geometries. It provides the complement to left4D, checking for strict right positioning relative to a query boundary. Along with other directional predicates (left4D, overLeft4D, overRight4D, below4D), it enables efficient spatial indexing and query processing for PostgreSQL's geometric data types. The function is declared static for internal use within geo_spgist.c.
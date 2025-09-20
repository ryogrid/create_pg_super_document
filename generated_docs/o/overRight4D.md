# overRight4D

## Location
[src/backend/utils/adt/geo_spgist.c:339-345](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L339-L345)

## Overview
Determines if any rectangle from a RectBox does not extend to the left of a given query boundary.

## Definition

```c
static bool
overRight4D(RectBox *rect_box, RangeBox *query)
```
## Detailed Description
This function is part of PostgreSQL's SP-GiST implementation for geometric box operations. It evaluates whether any rectangle within the provided RectBox structure could be positioned such that it does not extend beyond (to the left of) the specified query boundary. Unlike right4D which checks for strict right positioning, overRight4D allows for overlap at the boundary. The function uses the overHigher2D helper function to perform the x-axis range comparison with the query's left boundary.

## Parameters / Member Variables
- `rect_box`: Pointer to RectBox structure containing the spatial boundaries to be evaluated
- `query`: Pointer to RangeBox structure defining the query boundary conditions

## Dependencies
- Functions called/Symbols referenced:
  - [overHigher2D](overHigher2D.md)
  - RectBox (struct)
  - RangeBox (struct)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
This function is used in SP-GiST index operations for spatial queries involving box geometries. The 'over' prefix indicates that overlapping at the boundary is permitted, distinguishing it from the strict directional predicate right4D. It complements overLeft4D by checking the opposite directional relationship and is part of the comprehensive spatial relationship predicate system used for efficient geometric indexing and query processing in PostgreSQL.
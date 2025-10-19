# overLeft4D

## Location
[src/backend/utils/adt/geo_spgist.c:325-331](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L325-L331)

## Overview
Determines if any rectangle from a RectBox does not extend to the right of a given query boundary.

## Definition

```c
static bool
overLeft4D(RectBox *rect_box, RangeBox *query)
```
## Detailed Description
This function is part of PostgreSQL's SP-GiST implementation for geometric box operations. It evaluates whether any rectangle within the provided RectBox structure could be positioned such that it does not extend beyond (to the right of) the specified query boundary. Unlike left4D which checks for strict left positioning, overLeft4D allows for overlap at the boundary. The function uses the overLower2D helper function to perform the x-axis range comparison.

## Parameters / Member Variables
- `rect_box`: Pointer to RectBox structure containing the spatial boundaries to be evaluated
- `query`: Pointer to RangeBox structure defining the query boundary conditions

## Dependencies
- Functions called/Symbols referenced:
  - [overLower2D](overLower2D.md)
  - RectBox (struct)
  - [RangeBox](../R/RangeBox.md) (struct)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
This function is used in SP-GiST index operations for spatial queries involving box geometries. The 'over' prefix indicates that overlapping at the boundary is permitted, distinguishing it from the strict directional predicate left4D. It is part of a comprehensive set of spatial relationship predicates used for efficient geometric indexing and query processing in PostgreSQL's spatial data types.

## Simplified Source

```c
/* Check if any rectangle from rect_box doesn't extend right of query */
static bool
overLeft4D(RectBox *rect_box, RangeBox *query)
{
    // Use 2D function to check X-axis positioning
    return overLower2D(&rect_box->range_box_x, &query->left);
}
```
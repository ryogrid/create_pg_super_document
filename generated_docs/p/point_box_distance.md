# point_box_distance

## Location
[src/backend/access/spgist/spgproc.c:31-62](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgproc.c#L31-L62)

## Overview
Calculates the minimum distance between a point and an axis-aligned bounding box (BOX) in 2D space.

## Definition

```c
static double
point_box_distance(Point *point, BOX *box)
```
## Detailed Description
This function computes the Euclidean distance from a given point to the nearest point on or within an axis-aligned bounding box. The calculation follows these rules:
- If the point is inside the box, the distance is 0.0
- If the point is outside the box, it calculates the shortest distance to the box boundary
- Uses the HYPOT function to compute the Euclidean distance from the x and y components
- Handles NaN values by returning NaN if any coordinate contains NaN

The function is optimized for spatial indexing operations in PostgreSQL's SP-GiST (Space-Partitioned Generalized Search Tree) implementation.

## Parameters / Member Variables
- `*point`: Pointer to a Point structure containing x and y coordinates
- `*box`: Pointer to a BOX structure representing an axis-aligned bounding box with low and high corner points
## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) (data structure)
  - [BOX](../B/BOX.md) (data structure)  
  - isnan (NaN checking function)
  - [get_float8_nan](../g/get_float8_nan.md) (NaN value generator)
  - HYPOT (hypotenuse calculation macro)
- Called from (representative examples):
  - [spg_key_orderbys_distances](../s/spg_key_orderbys_distances.md)

## Notes and Other Information
- This is a static function used internally within the SP-GiST spatial indexing system
- The function assumes the box is axis-aligned (edges parallel to coordinate axes)
- NaN handling ensures robust behavior with invalid geometric data
- The distance calculation is used for nearest-neighbor queries and spatial ordering operations in PostgreSQL's geometric indexing

## Simplified Source

```c
static double point_box_distance(Point *point, BOX *box) {
    double dx, dy;

    // Handle NaN values in coordinates
    if (isnan(point->x) || isnan(box->low.x) ||
        isnan(point->y) || isnan(box->low.y))
        return get_float8_nan();

    // Calculate X distance component
    if (point->x < box->low.x)
        dx = box->low.x - point->x;        // Point is left of box
    else if (point->x > box->high.x)
        dx = point->x - box->high.x;       // Point is right of box
    else
        dx = 0.0;                          // Point is within X range

    // Calculate Y distance component
    if (point->y < box->low.y)
        dy = box->low.y - point->y;        // Point is below box
    else if (point->y > box->high.y)
        dy = point->y - box->high.y;       // Point is above box
    else
        dy = 0.0;                          // Point is within Y range

    // Return Euclidean distance (0.0 if point is inside box)
    return HYPOT(dx, dy);
}
```
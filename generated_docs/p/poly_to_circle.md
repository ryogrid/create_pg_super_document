# poly_to_circle

## Location
[src/backend/utils/adt/geo_ops.c:5285-5306](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5285-L5306)

## Overview
Converts a polygon to its approximate equivalent circle by calculating the centroid as the center and the average distance from vertices to center as the radius.

## Definition

```c
static void
poly_to_circle(CIRCLE *result, POLYGON *poly)
```
## Detailed Description
The `poly_to_circle` function performs a polygon-to-circle conversion using a simple averaging algorithm. It first calculates the polygon's centroid by averaging the coordinates of all vertices to determine the circle's center. Then it computes the average distance from all vertices to this center point to establish the circle's radius.

The function operates in two phases: first summing all vertex coordinates and dividing by the vertex count to find the center, then calculating the mean distance from each vertex to this center. The resulting circle provides a reasonable approximation of the polygon's size and position, though the algorithm has known limitations as noted in the source comments.

## Parameters / Member Variables
- `result` (CIRCLE*): Pointer to the output circle structure that will be filled with computed values (must be pre-allocated)
- `poly` (POLYGON*): Pointer to the input polygon structure containing vertices and vertex count

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro for debugging)
  - [point_add_point](point_add_point.md) (vector addition of two points)
  - [float8_div](../f/float8_div.md) (floating-point division)
  - [float8_pl](../f/float8_pl.md) (floating-point addition)
  - [point_dt](point_dt.md) (distance calculation between two points)
- Data types referenced:
  - CIRCLE, POLYGON (geometric data structures)

- Called from (representative examples):
  - [poly_center](poly_center.md) (calculates polygon center point)
  - [poly_circle](poly_circle.md) (public interface for polygon-to-circle conversion)

## Notes and Other Information
- This is a static (internal) function not directly exposed as a PostgreSQL function
- The algorithm uses simple arithmetic means which may not produce optimal results for all polygon shapes
- The source code includes a TODO comment suggesting the algorithm should use weighted means of line segments rather than straight vertex averaging for better accuracy
- The function assumes the input polygon has at least one vertex (enforced by Assert)
- The result parameter must be pre-allocated by the caller; this function only fills in the values
- For irregular polygons, the resulting circle may not closely approximate the original shape due to the simplistic averaging approach

## Simplified Source

```c
static void poly_to_circle(CIRCLE *result, POLYGON *poly) {
    Assert(poly->npts > 0);

    // Initialize result
    result->center.x = 0;
    result->center.y = 0;
    result->radius = 0;

    // Calculate center as average of all vertices
    for (int i = 0; i < poly->npts; i++) {
        result->center.x += poly->p[i].x;
        result->center.y += poly->p[i].y;
    }
    result->center.x /= poly->npts;
    result->center.y /= poly->npts;

    // Calculate radius as average distance from center to vertices
    for (int i = 0; i < poly->npts; i++) {
        result->radius += point_dt(&poly->p[i], &result->center);
    }
    result->radius /= poly->npts;
}
```
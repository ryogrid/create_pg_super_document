# dist_ppoly_internal

## Location
[src/backend/utils/adt/geo_ops.c:2630-2674](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2630-L2674)

## Overview
An internal static function that calculates the shortest distance between a point and a polygon, implementing the core distance computation logic.

## Definition

```c
struct(&tmp, &lseg->p[0], lseg_sl(lseg));
```
## Detailed Description
This function implements the core algorithm for calculating the minimum distance between a point and a polygon. It first checks if the point is inside the polygon using  - if so, it returns 0.0 since the distance is zero. If the point is outside, it calculates the distance from the point to each edge (line segment) of the polygon and returns the minimum distance found. The function iterates through all polygon edges, including the edge connecting the last vertex back to the first vertex to close the polygon.

## Parameters / Member Variables
- : Pointer to the Point for which distance is calculated
- : Pointer to the POLYGON structure containing the polygon data

## Dependencies
- Functions called/Symbols referenced:
  -  - Checks if point is inside polygon
  -  - Calculates distance from point to line segment
  -  - Compares two float8 values for less-than relationship
  -  - Line segment data structure
  -  - [Point](../P/Point.md) data structure  
  -  - Polygon data structure
- Called from (representative examples):
  -  - Distance from polygon to point
  -  - Distance from circle to polygon
  -  - Distance from point to polygon

## Notes and Other Information
- This is a static internal function, not directly accessible from SQL
- Handles the closed polygon case by explicitly checking the edge from last vertex to first vertex
- Returns 0.0 immediately if the point is inside the polygon
- Uses an iterative approach to find minimum distance across all polygon edges
- The algorithm ensures complete coverage of the polygon boundary for distance calculation

## Simplified Source

```c
static float8
dist_ppoly_internal(Point *pt, POLYGON *poly)
{
    float8 result, d;
    int i;
    LSEG seg;

    // If point is inside polygon, distance is zero
    if (point_inside(pt, poly->npts, poly->p) != 0)
        return 0.0;

    // Initialize with distance to edge from first to last vertex (close polygon)
    seg.p[0] = poly->p[0];
    seg.p[1] = poly->p[poly->npts - 1];
    result = lseg_closept_point(NULL, &seg, pt);

    // Check distance to each edge and keep minimum
    for (i = 0; i < poly->npts - 1; i++)
    {
        seg.p[0] = poly->p[i];
        seg.p[1] = poly->p[i + 1];
        d = lseg_closept_point(NULL, &seg, pt);
        if (d < result)
            result = d;
    }

    return result;
}
```
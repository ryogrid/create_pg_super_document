# on_ppath

## Location
[src/backend/utils/adt/geo_ops.c:3166-3200](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3166-L3200)

## Overview
This function determines whether a point lies on a path (polyline), using different algorithms depending on whether the path is open or closed.

## Definition

```c
Datum
on_ppath(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function tests whether a point lies within (on) a polyline path. It implements two different algorithms based on the path type:

For **open paths**: Uses a segment-by-segment check algorithm. It examines each line segment in the path to determine if the point lies exactly on any segment by using distance calculations and the triangle inequality principle.

For **closed paths**: Uses the classic ray-casting algorithm (point-in-polygon test). It casts a horizontal ray from the point to the right and counts intersections with path segments. An odd number of crossings indicates the point is inside the polygon.

The function handles edge cases where endpoints or edges may touch but not cross the ray.

## Parameters / Member Variables
- : PostgreSQL function call context containing:
  - Argument 0:  - The point to test for containment on the path
  - Argument 1:  - The path (polyline) to test against

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts point argument from function call
  -  - Extracts path argument from function call
  -  - Calculates distance between two points
  -  - Floating-point addition
  -  - Floating-point equality comparison with tolerance
  -  - [Point](../P/Point.md)-in-polygon test for closed paths
  -  - Returns boolean result to PostgreSQL
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Part of PostgreSQL's geometric data type operators for path operations
- Uses O(n) algorithm for both open and closed paths, though closed path algorithm could theoretically be optimized to O(log n) with preprocessing
- The open path algorithm uses the triangle inequality principle: a point lies on a line segment if the sum of distances from the point to both endpoints equals the length of the segment
- Located in src/backend/utils/adt/geo_ops.c:3166-3200
- The function name 'on_ppath' likely stands for 'on point-path'
- Includes detailed comments explaining the ray-casting algorithm implementation

## Simplified Source

```c
Datum on_ppath(PG_FUNCTION_ARGS) {
    // Extract point and path from function arguments
    Point *pt = PG_GETARG_POINT_P(0);
    PATH *path = PG_GETARG_PATH_P(1);

    // OPEN PATH: Check each segment
    if (!path->closed) {
        int n = path->npts - 1;
        float8 a = point_dt(pt, &path->p[0]);

        // Test if point lies on any line segment
        for (int i = 0; i < n; i++) {
            float8 b = point_dt(pt, &path->p[i + 1]);
            // Triangle inequality: point on segment if sum of distances equals segment length
            if (FPeq(float8_pl(a, b), point_dt(&path->p[i], &path->p[i + 1])))
                PG_RETURN_BOOL(true);
            a = b;
        }
        PG_RETURN_BOOL(false);
    }

    // CLOSED PATH: Use point-in-polygon test
    PG_RETURN_BOOL(point_inside(pt, path->npts, path->p) != 0);
}
```
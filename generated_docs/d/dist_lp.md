# dist_lp

## Location
[src/backend/utils/adt/geo_ops.c:2402-2413](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2402-L2413)

## Overview
Calculates the minimum distance from a line to a point in 2D space.

## Definition

```c
Datum
dist_lp(PG_FUNCTION_ARGS)
```
## Detailed Description
This PostgreSQL function computes the shortest distance between an infinite line and a point in 2D space. The function is essentially the same as dist_pl but with reversed parameter order (line first, then point). It uses the same internal line_closept_point() function to calculate the closest point on the line to the given point and returns the distance. This provides a symmetric interface for distance calculations between lines and points in PostgreSQL's geometric system.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LINE_P (retrieve line argument)
  - PG_GETARG_POINT_P (retrieve point argument)
  - [line_closept_point](../l/line_closept_point.md) (calculate closest point and distance between line and point)
  - PG_RETURN_FLOAT8 (return float8 result)
- Called from:
  - No direct references found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2402-2413
- Functionally identical to dist_pl but with parameter order reversed (line, point vs point, line)
- Part of PostgreSQL's geometric distance calculation routines for 2D objects
- The function is designed for infinite lines, not line segments
- Uses NULL as the first parameter to line_closept_point(), indicating only the distance is needed, not the actual closest point coordinates
- This function can be called from SQL queries as part of geometric distance operations
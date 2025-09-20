# path_area

## Location
[src/backend/utils/adt/geo_ops.c:1380-1401](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1380-L1401)

## Overview
Calculates the area enclosed by a closed path (polygon) using the shoelace formula, returning NULL for open paths.

## Definition

```c
Datum
path_area(PG_FUNCTION_ARGS)
```
## Detailed Description
This function computes the area of a closed path representing a polygon using the shoelace formula (also known as the surveyor's formula). The algorithm works by summing the cross products of consecutive vertices to calculate twice the signed area, then taking the absolute value and dividing by 2 to get the actual area. For open paths (polylines), the function returns NULL since area is only meaningful for closed polygons.

The shoelace formula implementation:
1. Iterates through each vertex of the path
2. For each vertex i, considers the next vertex j = (i+1) % npts (wrapping to first vertex)
3. Accumulates: area += (x_i * y_j) - (y_i * x_j)
4. Returns |area| / 2

## Parameters / Member Variables
- : PostgreSQL function argument macro that provides access to:
  - Argument 0: PATH object representing the polygon or polyline

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P (retrieves PATH argument)
  - [float8_pl](../f/float8_pl.md), float8_mi, float8_mul, float8_div (floating-point arithmetic)
  - fabs (absolute value)
  - PG_RETURN_NULL (returns NULL result)
  - PG_RETURN_FLOAT8 (returns float8 result)
- Types used:
  - [PATH](../P/PATH.md) (geometric path type with npts, closed flag, and point array)
  - float8 (double precision floating-point)
  - Datum (PostgreSQL data type)
- Called from:
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1380-1401
- Part of PostgreSQL's geometric data type operations for 2D paths
- Only works with closed paths; returns NULL for open paths
- Uses the mathematical shoelace formula for polygon area calculation
- Returns the absolute area value (always positive)
- Handles self-intersecting polygons by computing the absolute value of the signed area
- The PATH structure contains an array of points with coordinates accessible as path->p[i].x and path->p[i].y
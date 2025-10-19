# dist_ppath

## Location
[src/backend/utils/adt/geo_ops.c:2478-2489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2478-L2489)

## Overview
PostgreSQL SQL function that calculates the distance from a Point to a PATH (open or closed path/polygon).

## Definition

```c
Datum
dist_ppath(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the public PostgreSQL SQL interface for computing the shortest distance from a point to a path. It extracts the point and path arguments from the function call framework and delegates the actual distance calculation to . The function handles both open paths (series of connected line segments) and closed paths (polygons).

## Parameters / Member Variables
- : PostgreSQL function call framework arguments containing:
  - Argument 0: Point pointer () - the point from which to measure distance
  - Argument 1: PATH pointer () - the path to which distance is measured

## Dependencies
- Functions called/Symbols referenced:
  -  - PostgreSQL macro to extract Point argument
  -  - PostgreSQL macro to extract PATH argument
  -  - Internal function that performs the actual distance calculation
  -  - PostgreSQL macro to return float8 result
- Called from (representative examples):
  - No direct callers found in codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in 
- This is a PostgreSQL SQL callable function for geometric operations
- Acts as a thin wrapper around 
- Returns the result as a float8 (double precision) value
- Can handle both open and closed paths through the internal implementation
- Part of PostgreSQL's geometric data type system

## Simplified Source

```c
Datum dist_ppath(PG_FUNCTION_ARGS) {
    // Get point and path arguments
    Point *pt = PG_GETARG_POINT_P(0);
    PATH *path = PG_GETARG_PATH_P(1);

    // Calculate and return minimum distance from point to path
    PG_RETURN_FLOAT8(dist_ppath_internal(pt, path));
}
```
# dist_pb

## Location
src/backend/utils/adt/geo_ops.c: 2502 - 2513

## Overview
Calculates the shortest distance from a point to a box (rectangle) in 2D coordinate space.

## Definition
```c
Datum dist_pb(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dist_pb` function is a PostgreSQL built-in function that computes the minimum distance between a given point and a box. It utilizes the internal `box_closept_point` function to find the closest point on the box to the given point and returns the distance as a float8 value. This function is part of PostgreSQL's geometric data type operations and is used in spatial calculations and proximity queries.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `Point *pt` - The source point for distance calculation
  - Argument 1: `BOX *box` - The target box for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINT_P` - Extracts Point argument from function call
  - `PG_GETARG_BOX_P` - Extracts BOX argument from function call
  - `[box_closept_point](../b/box_closept_point.md)` - Computes closest point on box to given point
  - `PG_RETURN_FLOAT8` - Returns float8 result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2502-2513
- This function follows PostgreSQL's V1 calling convention
- Returns the actual distance value, not the squared distance
- Part of PostgreSQL's geometric operator family for spatial indexing and queries
- Complementary to `dist_bp` which calculates distance from box to point (same result due to symmetry)
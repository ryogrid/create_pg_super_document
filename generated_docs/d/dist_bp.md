# dist_bp

## Location
src/backend/utils/adt/geo_ops.c: 2514 - 2525

## Overview
Calculates the shortest distance from a box (rectangle) to a point in 2D coordinate space.

## Definition
```c
Datum dist_bp(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dist_bp` function is a PostgreSQL built-in function that computes the minimum distance between a given box and a point. Despite the reversed parameter order compared to `dist_pb`, it uses the same underlying `box_closept_point` function to calculate the distance. This function is part of PostgreSQL's comprehensive geometric data type operations and provides symmetric distance calculation capabilities for spatial queries.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `BOX *box` - The source box for distance calculation
  - Argument 1: `Point *pt` - The target point for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOX_P` - Extracts BOX argument from function call
  - `PG_GETARG_POINT_P` - Extracts Point argument from function call
  - [box_closept_point](../b/box_closept_point.md) - Computes closest point on box to given point
  - `PG_RETURN_FLOAT8` - Returns float8 result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2514-2525
- This function follows PostgreSQL's V1 calling convention
- Mathematically equivalent to `dist_pb` due to distance symmetry
- Provides flexibility in SQL queries where box-to-point distance syntax may be more natural
- Part of PostgreSQL's geometric operator family for spatial indexing and proximity queries
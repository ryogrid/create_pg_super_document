# dist_sl

## Location
src/backend/utils/adt/geo_ops.c: 2526 - 2537

## Overview
Calculates the shortest distance from a line segment (lseg) to an infinite line in 2D coordinate space.

## Definition
```c
Datum dist_sl(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dist_sl` function is a PostgreSQL built-in function that computes the minimum distance between a line segment and an infinite line. It leverages the `lseg_closept_line` function to determine the closest point between these geometric objects and returns the distance as a float8 value. This function is essential for spatial analysis involving linear geometric relationships and proximity calculations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `LSEG *lseg` - The source line segment for distance calculation
  - Argument 1: `LINE *line` - The target infinite line for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_LSEG_P` - Extracts LSEG argument from function call
  - `PG_GETARG_LINE_P` - Extracts LINE argument from function call
  - `lseg_closept_line` - Computes closest point between line segment and line
  - `PG_RETURN_FLOAT8` - Returns float8 result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2526-2537
- This function follows PostgreSQL's V1 calling convention
- Handles both cases where the line segment intersects the line (distance = 0) and where they don't
- Complementary to `dist_ls` which calculates distance from line to line segment
- Part of PostgreSQL's geometric operator suite for complex spatial queries and geometric indexing
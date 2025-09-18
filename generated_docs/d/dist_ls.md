# dist_ls

## Location
src/backend/utils/adt/geo_ops.c: 2538 - 2549

## Overview
Calculates the shortest distance from an infinite line to a line segment (lseg) in 2D coordinate space.

## Definition
```c
Datum dist_ls(PG_FUNCTION_ARGS)
```

## Detailed Description
The `dist_ls` function is a PostgreSQL built-in function that computes the minimum distance between an infinite line and a line segment. Despite the reversed parameter order compared to `dist_sl`, it uses the same underlying `lseg_closept_line` function to calculate the distance. This function provides symmetric distance calculation capabilities for linear geometric objects in PostgreSQL's spatial data operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function call context containing:
  - Argument 0: `LINE *line` - The source infinite line for distance calculation
  - Argument 1: `LSEG *lseg` - The target line segment for distance calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_LINE_P` - Extracts LINE argument from function call
  - `PG_GETARG_LSEG_P` - Extracts LSEG argument from function call
  - `lseg_closept_line` - Computes closest point between line segment and line
  - `PG_RETURN_FLOAT8` - Returns float8 result
- Called from (representative examples):
  - No direct callers found in the analyzed codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2538-2549
- This function follows PostgreSQL's V1 calling convention
- Mathematically equivalent to `dist_sl` due to distance symmetry
- Provides SQL syntax flexibility when line-to-segment distance calculation is more natural
- Essential for geometric queries involving infinite lines and bounded line segments
- Part of PostgreSQL's comprehensive geometric data type operator family
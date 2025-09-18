# close_ls

## Location
src/backend/utils/adt/geo_ops.c: 2988 - 3012

## Overview
The `close_ls` function calculates the closest point on a line segment to an infinite line, returning the coordinates of that closest point.

## Definition
```c
Datum close_ls(PG_FUNCTION_ARGS)
```

## Detailed Description
This PostgreSQL built-in function computes the point on a line segment that is closest to an infinite line. It first checks if the line and line segment are parallel by comparing their slopes - if they are parallel, it returns NULL since there is no single closest point. For non-parallel cases, it uses the `lseg_closept_line` helper function to compute the actual closest point. The function handles edge cases by checking for NaN results and returning NULL when appropriate.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `LINE *line` - The infinite line to measure distance to
  - Argument 1: `LSEG *lseg` - The line segment to find the closest point on

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_LINE_P` - Extracts line argument from function parameters
  - `PG_GETARG_LSEG_P` - Extracts line segment argument from function parameters
  - `LINE` - Line data type definition
  - `LSEG` - Line segment data type definition
  - `Point` - Point data type definition
  - `lseg_sl` - Gets slope of line segment
  - `line_sl` - Gets slope of line
  - `palloc` - PostgreSQL memory allocation function
  - `lseg_closept_line` - Core geometric function that performs closest point calculation
  - `isnan` - Standard C library function to check for NaN values
  - `PG_RETURN_POINT_P` - Returns point result from PostgreSQL function
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations
- The function handles parallel lines by comparing slopes and returning NULL in such cases
- Returns NULL for parallel geometries since there is no unique closest point
- The function properly handles edge cases by checking for NaN results
- Memory for the result point is allocated using PostgreSQL's memory management system
- The actual geometric computation is delegated to the `lseg_closept_line` helper function
- Located in the geometric operations module (`geo_ops.c`) at lines 2988-3012
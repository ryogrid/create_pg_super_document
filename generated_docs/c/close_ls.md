# close_ls

## Location
[src/backend/utils/adt/geo_ops.c:2988-3012](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2988-L3012)

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
  - [LSEG](../L/LSEG.md) - Line segment data type definition
  - [Point](../P/Point.md) - [Point](../P/Point.md) data type definition
  - [lseg_sl](../l/lseg_sl.md) - Gets slope of line segment
  - [line_sl](../l/line_sl.md) - Gets slope of line
  - [palloc](../p/palloc.md) - PostgreSQL memory allocation function
  - [lseg_closept_line](../l/lseg_closept_line.md) - Core geometric function that performs closest point calculation
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

## Simplified Source

```c
Datum close_ls(PG_FUNCTION_ARGS) {
    LINE *line = PG_GETARG_LINE_P(0);
    LSEG *lseg = PG_GETARG_LSEG_P(1);
    Point *result;

    // Check if line and segment are parallel
    if (lseg_sl(lseg) == line_sl(line))
        PG_RETURN_NULL();

    // Allocate memory for result point
    result = (Point *) palloc(sizeof(Point));

    // Calculate closest point on segment to line
    if (isnan(lseg_closept_line(result, lseg, line)))
        PG_RETURN_NULL();

    PG_RETURN_POINT_P(result);
}
```
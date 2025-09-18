# lseg_length

## Location
src/backend/utils/adt/geo_ops.c: 2172 - 2187

## Overview
Calculates and returns the length (distance) of a line segment as a PostgreSQL function.

## Definition
```c
Datum lseg_length(PG_FUNCTION_ARGS)
```

## Detailed Description
The `lseg_length` function is a PostgreSQL built-in function that calculates the length of a line segment. It extracts the line segment from the function arguments using the PostgreSQL function interface, then computes the Euclidean distance between the segment's two endpoints using the `point_dt` function. The result is returned as a PostgreSQL float8 (double precision) value. This function implements the SQL function that can be called from SQL queries to get the length of a line segment.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention, where the line segment is retrieved as the first argument using PG_GETARG_LSEG_P(0)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSEG_P: PostgreSQL macro to extract line segment argument
  - point_dt: Calculates Euclidean distance between two points using HYPOT
  - PG_RETURN_FLOAT8: PostgreSQL macro to return float8 result
  - LSEG: Line segment data structure type

- Called from (representative examples):
  - This function is typically called from SQL queries rather than C code
  - No direct C references found in the current codebase

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL as lseg_length()
- Uses the HYPOT function internally for precise calculation of the hypotenuse (Euclidean distance)
- Returns the length as a double precision floating point number
- Part of PostgreSQL's geometric data type support
- The calculation is performed using the formula: sqrt((x2-x1)² + (y2-y1)²)
# close_lseg

## Location
[src/backend/utils/adt/geo_ops.c:2853-2877](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2853-L2877)

## Overview
PostgreSQL SQL function that returns the closest point between two line segments.

## Definition


## Detailed Description
This function serves as the PostgreSQL SQL interface for finding the closest point between two line segments. It first checks if the line segments have the same slope (are parallel), in which case it returns NULL since parallel lines either don't have a meaningful closest point or are the same line. For non-parallel segments, it uses the internal lseg_closept_lseg function to calculate the closest points and handles potential NaN results by returning NULL when the calculation is invalid.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro to access arguments:
  - Argument 0: LSEG structure representing the first line segment
  - Argument 1: LSEG structure representing the second line segment
- Returns: Point structure containing the closest point coordinates, or NULL if segments are parallel or calculation fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSEG_P: Extracts line segment arguments from function call
  - [lseg_sl](../l/lseg_sl.md): Calculates the slope of a line segment
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [lseg_closept_lseg](../l/lseg_closept_lseg.md): Internal function that performs the closest point calculation between line segments
  - isnan: Checks for NaN (Not a Number) values
  - PG_RETURN_POINT_P: Returns point result to SQL layer
- Called from (representative examples):
  - This function is exposed as a SQL function and called from SQL queries

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL
- Function signature in SQL would be close_lseg(lseg, lseg) returning point
- Specifically handles the parallel line case by checking if slopes are equal
- Allocates memory using palloc for the result point
- Properly handles edge cases by returning NULL for parallel segments or invalid calculations
- Part of PostgreSQL's geometric data type system for 2D geometry operations
- Returns NULL when line segments are parallel, as the concept of "closest point" is not well-defined for parallel lines
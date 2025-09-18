# close_ps

## Location
src/backend/utils/adt/geo_ops.c: 2791 - 2809

## Overview
PostgreSQL SQL function that returns the closest point on a line segment to a given point.

## Definition


## Detailed Description
This function serves as the PostgreSQL SQL interface for finding the closest point on a line segment to a specified point. It takes a point and a line segment as arguments, allocates memory for the result point, and uses the internal lseg_closept_point function to perform the actual geometric calculation. The function handles potential NaN (Not a Number) results by returning NULL when the calculation is invalid.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS macro to access arguments:
  - Argument 0: Point structure representing the input point
  - Argument 1: LSEG structure representing the line segment
- Returns: Point structure containing the closest point coordinates, or NULL if calculation fails

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P: Extracts point argument from function call
  - PG_GETARG_LSEG_P: Extracts line segment argument from function call
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [lseg_closept_point](../l/lseg_closept_point.md): Internal function that performs the closest point calculation
  - isnan: Checks for NaN (Not a Number) values
  - PG_RETURN_POINT_P: Returns point result to SQL layer
- Called from (representative examples):
  - This function is exposed as a SQL function and called from SQL queries

## Notes and Other Information
- This is a PostgreSQL built-in function callable from SQL
- Function signature in SQL would be close_ps(point, lseg) returning point
- Allocates memory using palloc for the result point
- Properly handles edge cases by returning NULL for invalid calculations
- Part of PostgreSQL's geometric data type system for 2D geometry operations
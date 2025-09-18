# interval_mi

## Location
src/backend/utils/adt/timestamp.c: 3518 - 3566

## Overview
A PostgreSQL function that implements interval subtraction operation (span1 - span2) with support for infinite intervals.

## Definition


## Detailed Description
This function is the main entry point for interval subtraction in PostgreSQL's SQL interface. It handles the subtraction of two intervals, including special cases involving infinite intervals. The function properly handles the mathematical edge cases of infinity arithmetic:

- infinity - infinity = error (undefined)
- infinity - finite = infinity  
- finite - infinity = -infinity
- finite - finite = computed difference

When both intervals are finite, the function delegates to  for safe arithmetic computation. The function follows PostgreSQL's function calling convention using PG_FUNCTION_ARGS and returns a Datum.

## Parameters / Member Variables
- Function uses  calling convention:
  - Argument 0: First interval (span1) - the minuend
  - Argument 1: Second interval (span2) - the subtrahend
- Returns: Datum containing the resulting interval

## Dependencies
- Functions called/Symbols referenced:
  -  (extract interval arguments)
  -  (memory allocation)
  -  (check for negative infinity)
  -  (check for positive infinity)
  -  (set negative infinity)
  -  (set positive infinity)
  -  (perform finite interval subtraction)
  -  (return interval result)
  -  (error reporting)
- Called from (representative examples):
  -  (interval linear interpolation)
  -  (range checking for intervals)

## Notes and Other Information
- This is a PostgreSQL V1 calling convention function, accessible from SQL as the '-' operator for intervals
- Allocates memory for the result using 
- Treats "infinity - infinity" as an error since intervals don't have NaN equivalent
- Follows mathematical rules for infinity arithmetic in other cases
- Part of PostgreSQL's timestamp/interval arithmetic system
- Located in src/backend/utils/adt/timestamp.c:3518-3566
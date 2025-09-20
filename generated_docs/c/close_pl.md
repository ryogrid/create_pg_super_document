# close_pl

## Location
[src/backend/utils/adt/geo_ops.c:2750-2771](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2750-L2771)

## Overview
A PostgreSQL function that returns the closest point on a line to a given point, serving as a SQL-callable wrapper for point-to-line closest point calculation.

## Definition

```c
struct(&tmp, pt, point_invsl(&lseg->p[0], &lseg->p[1]));
```
## Detailed Description
This function implements the SQL-callable interface for finding the closest point on an infinite line to a given point. It extracts a Point and LINE from the function arguments, allocates memory for the result point, and delegates the actual computation to . If the internal function returns NaN (indicating a computation error), the function returns NULL to SQL. Otherwise, it returns the computed closest point. This function follows PostgreSQL's standard function calling convention and provides proper memory management for the result.

## Parameters / Member Variables
- Function uses  macro which provides access to:
  - Argument 0:  - The point from which to find the closest point on the line
  - Argument 1:  - The infinite line on which to find the closest point

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts point argument from function call
  -  - Extracts line argument from function call
  -  - Allocates memory for result point
  -  - Performs actual closest point calculation
  -  - Checks if result is NaN (error condition)
  -  - Returns point result to SQL
  -  - Returns NULL to SQL for error cases
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts point argument
  -  - Extracts line argument
  -  - Memory allocation for result
  -  - Core closest point computation
  -  - NaN detection for error handling
  -  - Returns point result
  -  - Returns NULL for errors

## Notes and Other Information
- This is a SQL-callable function accessible through PostgreSQL's geometric functions
- Handles error conditions by returning NULL when computation fails
- Allocates memory for the result point using PostgreSQL's memory management
- Serves as a wrapper around the internal  function
- Part of PostgreSQL's geometric operations suite for point-line proximity calculations
- The function name follows PostgreSQL's geometric function naming convention (operation_type1_type2)
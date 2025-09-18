# point_div

## Location
[src/backend/utils/adt/geo_ops.c:4196-4216](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4196-L4216)

## Overview
The point_div function performs complex division of two Point geometric objects, serving as a PostgreSQL function interface for point division operations.

## Definition
Datum point_div(PG_FUNCTION_ARGS)

## Detailed Description
This function provides a PostgreSQL-callable interface for dividing two Point objects using complex number division. It extracts two Point arguments from the PostgreSQL function call, allocates memory for a result Point, and delegates the actual division computation to the point_div_point helper function. The function treats points as complex numbers where the x-coordinate represents the real part and the y-coordinate represents the imaginary part, performing division according to complex arithmetic rules.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function arguments containing:
  - Argument 0: First Point (p1) - the dividend
  - Argument 1: Second Point (p2) - the divisor

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (argument extraction)
  - [palloc](palloc.md) (memory allocation)
  - [point_div_point](point_div_point.md) (performs the actual division)
  - PG_RETURN_POINT_P (return value packaging)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- The function allocates memory for a new Point result using palloc
- Uses the point_div_point helper function for the mathematical computation
- Returns a PostgreSQL Datum containing the result Point
- Part of PostgreSQL's geometric data type operations
- No explicit division by zero handling at this level
- Located in src/backend/utils/adt/geo_ops.c at lines 4196-4216
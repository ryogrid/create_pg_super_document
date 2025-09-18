# point_mul

## Location
src/backend/utils/adt/geo_ops.c: 4167 - 4181

## Overview
The point_mul function performs complex multiplication of two Point geometric objects, treating them as complex numbers in a 2D coordinate system.

## Definition


## Detailed Description
This function implements complex number multiplication for PostgreSQL Point data types. It takes two Point objects as arguments and returns a new Point representing their complex multiplication. The function serves as a PostgreSQL function wrapper around the static helper function point_mul_point, which performs the actual mathematical computation. Complex multiplication treats each point as a complex number where x is the real part and y is the imaginary part, computing (a+bi) * (c+di) = (ac-bd) + (ad+bc)i.

## Parameters / Member Variables
- : Standard PostgreSQL function arguments containing:
  - Argument 0: First Point (p1) - the multiplicand
  - Argument 1: Second Point (p2) - the multiplier

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (argument extraction)
  - palloc (memory allocation)
  - point_mul_point (performs the actual multiplication)
  - PG_RETURN_POINT_P (return value packaging)
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- The function allocates memory for a new Point result using palloc
- Uses the point_mul_point helper function for the mathematical computation
- Returns a PostgreSQL Datum containing the result Point
- Part of PostgreSQL's geometric data type operations
- Located in src/backend/utils/adt/geo_ops.c at lines 4167-4181
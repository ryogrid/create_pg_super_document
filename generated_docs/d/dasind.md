# dasind

## Location
src/backend/utils/adt/float.c: 2138 - 2174

## Overview
The  function computes the inverse sine (arcsine) of a floating-point value and returns the result in degrees rather than radians.

## Definition


## Detailed Description
This function implements the PostgreSQL SQL function  with degree output. It takes a single floating-point argument and computes its inverse sine, returning the result in degrees within the range [-90, 90]. The function includes comprehensive input validation and error handling:

- Validates that the input is within the mathematical domain [-1, 1] for inverse sine
- Handles NaN inputs by returning NaN as per POSIX specification
- Uses optimized computation via the  helper function for values in the first quadrant
- Leverages symmetry properties (asin(-x) = -asin(x)) to handle negative inputs efficiently

## Parameters / Member Variables
- : The floating-point input value for which to compute the inverse sine in degrees (must be in range [-1, 1])

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call
  - isnan: Checks if input is Not-a-Number
  - get_float8_nan: Returns NaN value for float8
  - INIT_DEGREE_CONSTANTS: Initializes degree conversion constants
  - asind_q1: Computes inverse sine in degrees for first quadrant values [0, 1]
  - isinf: Checks if result is infinite
  - float_overflow_error: Reports overflow error
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- The function strictly enforces the mathematical domain [-1, 1] and raises a NUMERIC_VALUE_OUT_OF_RANGE error for invalid inputs
- Uses the helper function  for optimized computation in the first quadrant and applies symmetry for negative values
- Part of PostgreSQL's mathematical function library located in src/backend/utils/adt/float.c:2138-2174
- Includes overflow checking to ensure finite results
- Implements the SQL standard ASIN function with degree output rather than radian output
# dasind

## Location
[src/backend/utils/adt/float.c:2138-2174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2138-L2174)

## Overview
The  function computes the inverse sine (arcsine) of a floating-point value and returns the result in degrees rather than radians.

## Definition

```c
Datum
dasind(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL SQL function  with degree output. It takes a single floating-point argument and computes its inverse sine, returning the result in degrees within the range [-90, 90]. The function includes comprehensive input validation and error handling:

- Validates that the input is within the mathematical domain [-1, 1] for inverse sine
- Handles NaN inputs by returning NaN as per POSIX specification
- Uses optimized computation via the  helper function for values in the first quadrant
- Leverages symmetry properties (asin(-x) = -asin(x)) to handle negative inputs efficiently

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The floating-point input value for which to compute the inverse sine in degrees (must be in range [-1, 1])
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call
  - isnan: Checks if input is Not-a-Number
  - [get_float8_nan](../g/get_float8_nan.md): Returns NaN value for float8
  - INIT_DEGREE_CONSTANTS: Initializes degree conversion constants
  - [asind_q1](../a/asind_q1.md): Computes inverse sine in degrees for first quadrant values [0, 1]
  - isinf: Checks if result is infinite
  - [float_overflow_error](../f/float_overflow_error.md): Reports overflow error
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- The function strictly enforces the mathematical domain [-1, 1] and raises a NUMERIC_VALUE_OUT_OF_RANGE error for invalid inputs
- Uses the helper function  for optimized computation in the first quadrant and applies symmetry for negative values
- Part of PostgreSQL's mathematical function library located in src/backend/utils/adt/float.c:2138-2174
- Includes overflow checking to ensure finite results
- Implements the SQL standard ASIN function with degree output rather than radian output

## Simplified Source

```c
Datum dasind(PG_FUNCTION_ARGS) {
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 result;

    // Return NaN if input is NaN (POSIX compliance)
    if (isnan(arg1))
        PG_RETURN_FLOAT8(get_float8_nan());

    INIT_DEGREE_CONSTANTS();

    // Validate input range [-1, 1] for inverse sine
    if (arg1 < -1.0 || arg1 > 1.0)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("input is out of range")));

    // Calculate arcsine in degrees using symmetry
    if (arg1 >= 0.0)
        result = asind_q1(arg1);        // Direct calculation for non-negative
    else
        result = -asind_q1(-arg1);      // Use symmetry: asin(-x) = -asin(x)

    // Check for overflow (should not occur with valid inputs)
    if (unlikely(isinf(result)))
        float_overflow_error();

    PG_RETURN_FLOAT8(result);
}
```
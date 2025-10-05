# dacosd

## Location
[src/backend/utils/adt/float.c:2101-2137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2101-L2137)

## Overview
A PostgreSQL function that calculates the arccosine (inverse cosine) of a floating-point number and returns the result in degrees.

## Definition

```c
Datum
dacosd(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function computes the inverse cosine of a floating-point argument and returns the result in degrees rather than radians. It implements the principal branch of the arccosine function, mapping inputs in the range [-1, 1] to outputs in the range [0, 180] degrees. The function uses PostgreSQL's specialized degree-based trigonometric helper functions ( and ) to ensure accurate results, particularly for common angle values. It includes comprehensive error handling for invalid inputs and follows POSIX specifications for NaN handling.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: A float8 (double precision) input value in the range [-1, 1] representing the cosine value for which to find the angle
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract the float8 argument)
  - isnan (to check for NaN input)
  - [get_float8_nan](../g/get_float8_nan.md) (to return NaN when input is NaN)
  - INIT_DEGREE_CONSTANTS (macro to initialize degree constants)
  - [acosd_q1](../a/acosd_q1.md) (PostgreSQL arccosine function for first quadrant)
  - [asind_q1](../a/asind_q1.md) (PostgreSQL arcsine function for first quadrant)
  - isinf (to check for infinite result)
  - [float_overflow_error](../f/float_overflow_error.md) (for overflow error reporting)
  - ereport (for error reporting)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:2101-2137
- Returns NaN if the input is NaN, following POSIX specifications
- Validates input range [-1, 1] and throws error for out-of-range values
- Uses different calculation strategies for non-negative (acosd_q1) and negative (90° + asind_q1(-arg1)) inputs
- Part of PostgreSQL's degree-based trigonometric function suite for improved accuracy
- The result is always finite for valid inputs within the specified range
- Initializes degree constants on first use via INIT_DEGREE_CONSTANTS macro

## Simplified Source

```c
Datum dacosd(PG_FUNCTION_ARGS) {
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 result;

    // Return NaN if input is NaN (POSIX compliance)
    if (isnan(arg1))
        PG_RETURN_FLOAT8(get_float8_nan());

    INIT_DEGREE_CONSTANTS();

    // Validate input range [-1, 1] for inverse cosine
    if (arg1 < -1.0 || arg1 > 1.0)
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("input is out of range")));

    // Calculate arccosine in degrees using specialized functions
    if (arg1 >= 0.0)
        result = acosd_q1(arg1);          // Direct calculation for non-negative
    else
        result = 90.0 + asind_q1(-arg1);  // Use identity for negative values

    // Check for overflow (should not occur with valid inputs)
    if (unlikely(isinf(result)))
        float_overflow_error();

    PG_RETURN_FLOAT8(result);
}
```
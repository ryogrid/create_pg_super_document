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
- : A float8 (double precision) input value in the range [-1, 1] representing the cosine value for which to find the angle

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (to extract the float8 argument)
  - isnan (to check for NaN input)
  - get_float8_nan (to return NaN when input is NaN)
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
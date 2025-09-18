# dsin

## Location
[src/backend/utils/adt/float.c:1931-1957](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1931-L1957)

## Overview
The dsin function computes the sine of a floating-point argument given in radians and returns the trigonometric sine value.

## Definition
```c
Datum dsin(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL SIN function for PostgreSQL, computing the sine of the input value. The implementation follows the same error handling pattern as the dcos() function, including comprehensive checks for special values and error conditions. The function uses errno-based error detection to catch platform-specific issues that may arise with very large inputs where precision is compromised. Like other trigonometric functions, it explicitly handles infinite inputs and reports appropriate domain errors as required by POSIX specifications.

## Parameters / Member Variables
- `arg1`: The floating-point input value in radians for which to compute the sine

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8
  - isnan
  - get_float8_nan
  - sin (standard C library function)
  - isinf (called twice)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [float_overflow_error](../f/float_overflow_error.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns NaN if the input is NaN, per POSIX specification
- Follows the same error handling pattern as dcos() for consistency
- Explicitly checks errno for platform-specific error conditions
- Reports domain errors for infinite inputs as required by POSIX
- Includes overflow checking for the computed result
- The periodic nature of sine allows it to theoretically work with all finite inputs
- Located in src/backend/utils/adt/float.c:1931-1957
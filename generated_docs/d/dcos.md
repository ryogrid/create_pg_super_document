# dcos

## Location
[src/backend/utils/adt/float.c:1862-1902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1862-L1902)

## Overview
The dcos function computes the cosine of a floating-point argument given in radians and returns the trigonometric cosine value.

## Definition
```c
Datum dcos(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL COS function for PostgreSQL, computing the cosine of the input value. The function includes comprehensive error handling for edge cases, following POSIX specifications. Since cosine is periodic, it can theoretically handle all finite inputs, but some implementations may report errors for very large inputs where precision is lost. The function explicitly checks for infinite inputs and reports domain errors as required by POSIX. Error detection is performed through errno checking, as some platforms may not reliably report errors through other mechanisms.

## Parameters / Member Variables
- `arg1`: The floating-point input value in radians for which to compute the cosine

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8
  - isnan
  - [get_float8_nan](../g/get_float8_nan.md)
  - cos (standard C library function)
  - isinf (called twice)
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - [float_overflow_error](../f/float_overflow_error.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns NaN if the input is NaN, per POSIX specification
- Explicitly checks errno for platform-specific error reporting
- Reports domain errors for infinite inputs as required by POSIX
- Includes detailed comments about platform-specific error handling considerations
- The periodic nature of cosine allows it to work with most finite inputs
- Located in src/backend/utils/adt/float.c:1862-1902
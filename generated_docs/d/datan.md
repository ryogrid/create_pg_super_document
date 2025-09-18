# datan

## Location
[src/backend/utils/adt/float.c:1810-1835](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1810-L1835)

## Overview
The datan function computes the inverse tangent (arctangent) of a floating-point argument and returns the result in radians.

## Definition
```c
Datum datan(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the SQL ATAN function for PostgreSQL, computing the inverse tangent of the input value. It follows POSIX specifications for handling special cases like NaN inputs. The function maps all inputs to the principal branch of the inverse tangent function, returning values in the range [-π/2, π/2]. The result is always finite, even when the input is infinite, due to the mathematical properties of the arctangent function.

## Parameters / Member Variables
- `arg1`: The floating-point input value for which to compute the arctangent

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8
  - isnan
  - get_float8_nan
  - atan (standard C library function)
  - isinf
  - [float_overflow_error](../f/float_overflow_error.md)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Returns NaN if the input is NaN, per POSIX specification
- The principal branch ensures results are always in [-π/2, π/2]
- Includes overflow checking as a safety measure, though mathematically unnecessary for arctangent
- Located in src/backend/utils/adt/float.c:1810-1835
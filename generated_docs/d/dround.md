# dround

## Location
[src/backend/utils/adt/float.c:1361-1372](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1361-L1372)

## Overview
Implements the ROUND function for double-precision floating-point numbers (float8), rounding to the nearest integer.

## Definition
```c
Datum dround(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL built-in ROUND function for float8 (double-precision floating-point) values. It uses the standard C library function `rint()` to perform rounding to the nearest integer value. The function follows the PostgreSQL function calling convention using the fmgr interface and returns the result as a float8 value (not converted to integer type).

## Parameters / Member Variables
- Takes one argument accessed via `PG_GETARG_FLOAT8(0)`: The float8 value to be rounded to the nearest integer

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro for retrieving float8 argument)
  - PG_RETURN_FLOAT8 (macro for returning float8 result)
  - rint() (C library function for rounding to nearest integer)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1361-1372
- Part of the "RANDOM FLOAT8 OPERATORS" section in the source file
- Uses `rint()` which rounds to the nearest integer using the current rounding mode
- The result is returned as float8, not as an integer type
- This is different from functions that truncate toward zero - it rounds to the nearest integer value
- The rounding behavior follows IEEE 754 standards (typically "round half to even" or banker's rounding)

## Simplified Source

```c
Datum dround(PG_FUNCTION_ARGS) {
    float8 arg1 = PG_GETARG_FLOAT8(0);

    // Round to nearest integer using rint()
    PG_RETURN_FLOAT8(rint(arg1));
}
```
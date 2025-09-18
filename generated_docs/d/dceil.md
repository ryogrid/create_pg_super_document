# dceil

## Location
[src/backend/utils/adt/float.c:1373-1384](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1373-L1384)

## Overview
Implements the CEIL function for double-precision floating-point numbers (float8), returning the smallest integer greater than or equal to the input value.

## Definition
```c
Datum dceil(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL built-in CEIL (ceiling) function for float8 (double-precision floating-point) values. It uses the standard C library function `ceil()` to find the smallest integer value that is greater than or equal to the input. The function follows the PostgreSQL function calling convention using the fmgr interface and returns the result as a float8 value.

## Parameters / Member Variables
- Takes one argument accessed via `PG_GETARG_FLOAT8(0)`: The float8 value for which to find the ceiling

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro for retrieving float8 argument)
  - PG_RETURN_FLOAT8 (macro for returning float8 result)
  - ceil() (C library function for ceiling operation)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1373-1384
- Part of the "RANDOM FLOAT8 OPERATORS" section in the source file
- Uses `ceil()` which always rounds upward to the next integer
- Examples: ceil(2.3) = 3.0, ceil(-2.3) = -2.0, ceil(5.0) = 5.0
- The result is returned as float8, not as an integer type
- Complements `dfloor` (which rounds downward) and `dround` (which rounds to nearest)
- Follows IEEE 754 standards for ceiling operations
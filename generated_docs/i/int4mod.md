# int4mod

## Location
[src/backend/utils/adt/int.c:1130-1157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1130-L1157)

## Overview
Computes the modulo (remainder) of two 32-bit integers, handling division-by-zero and the special case of INT_MIN % -1.

## Definition
```c
Datum int4mod(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int4mod` function implements the modulo operation between two 32-bit integers (int4), returning the remainder as a 32-bit integer. The function handles edge cases including division by zero (raises ERRCODE_DIVISION_BY_ZERO) and the special case of any number modulo -1, which always returns 0. Some hardware platforms may throw floating-point exceptions for INT_MIN % -1, so this case is handled explicitly to return the mathematically correct result of 0.

## Parameters / Member Variables
- `arg1`: The 32-bit integer dividend (number being divided)
- `arg2`: The 32-bit integer divisor (number dividing by)

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_INT32`: Extracts both 32-bit integer arguments
  - `ereport`: Reports division by zero errors
  - `PG_RETURN_INT32`: Returns the 32-bit result
  - `PG_RETURN_NULL`: Used as unreachable code marker after division by zero error
- Called from: No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operator implementation for 32-bit integers
- The function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Division by zero is explicitly checked and reported as an error
- The special case of modulo -1 is handled to avoid platform-specific floating-point exceptions
- Modulo -1 always mathematically equals 0, which is returned directly
- No general overflow checking is needed for modulo operations
- The function includes a compiler hint (PG_RETURN_NULL after division by zero error) to help with optimization
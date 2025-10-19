# dfloor

## Location
[src/backend/utils/adt/float.c:1385-1397](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1385-L1397)

## Overview
Implements the FLOOR function for double-precision floating-point numbers (float8), returning the largest integer less than or equal to the input value.

## Definition
```c
Datum dfloor(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL built-in FLOOR function for float8 (double-precision floating-point) values. It uses the standard C library function `floor()` to find the largest integer value that is less than or equal to the input. The function follows the PostgreSQL function calling convention using the fmgr interface and returns the result as a float8 value.

## Parameters / Member Variables
- Takes one argument accessed via `PG_GETARG_FLOAT8(0)`: The float8 value for which to find the floor

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro for retrieving float8 argument)
  - PG_RETURN_FLOAT8 (macro for returning float8 result)
  - floor() (C library function for floor operation)

- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:1385-1397
- Part of the "RANDOM FLOAT8 OPERATORS" section in the source file
- Uses `floor()` which always rounds downward to the previous integer
- Examples: floor(2.7) = 2.0, floor(-2.3) = -3.0, floor(5.0) = 5.0
- The result is returned as float8, not as an integer type
- Complements `dceil` (which rounds upward) and `dround` (which rounds to nearest)
- Follows IEEE 754 standards for floor operations
- Note the behavioral difference with negative numbers: floor(-2.3) = -3.0, not -2.0

## Simplified Source

```c
Datum dfloor(PG_FUNCTION_ARGS) {
    float8 arg1 = PG_GETARG_FLOAT8(0);

    // Return largest integer <= input
    PG_RETURN_FLOAT8(floor(arg1));
}
```
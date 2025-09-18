# int4shl

## Location
src/backend/utils/adt/int.c: 1420 - 1428

## Overview
Performs bitwise left shift operation on a 32-bit integer value by a specified number of positions and returns the result as a PostgreSQL function.

## Definition
```c
Datum int4shl(PG_FUNCTION_ARGS)
```

## Detailed Description
The int4shl function implements the bitwise left shift operator (<<) for PostgreSQL's integer type (int4). It takes two 32-bit signed integer arguments: the value to be shifted and the number of positions to shift left. This function is part of PostgreSQL's built-in integer bitwise operations and shifts the bits of the first argument to the left by the number of positions specified in the second argument.

The function uses PostgreSQL's function call convention with PG_FUNCTION_ARGS macro to access arguments and PG_RETURN_INT32 macro to return the result in the proper Datum format.

## Parameters / Member Variables
- `arg1` (PG_GETARG_INT32(0)): The 32-bit signed integer value to be left-shifted
- `arg2` (PG_GETARG_INT32(1)): The number of bit positions to shift left

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting int32 arguments)
  - PG_RETURN_INT32 (macro for returning int32 result)
- Called from (representative examples):
  - SQL queries using bitwise left shift operations on integer values
  - Internal PostgreSQL operator evaluation system

## Notes and Other Information
- This function is located in src/backend/utils/adt/int.c:1420-1428
- Part of PostgreSQL's arithmetic and bitwise operations for the int4 data type
- The left shift operation multiplies the value by 2^n where n is the shift amount
- Result follows standard C left shift semantics for 32-bit signed integers
- Behavior is undefined for negative shift amounts or shift amounts >= 32
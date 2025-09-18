# int4not

## Location
src/backend/utils/adt/int.c: 1438 - 1445

## Overview
Performs bitwise NOT (complement) operation on a 32-bit integer value and returns the result as a PostgreSQL function.

## Definition
```c
Datum int4not(PG_FUNCTION_ARGS)
```

## Detailed Description
The int4not function implements the bitwise NOT operator (~) for PostgreSQL's integer type (int4). It takes a single 32-bit signed integer argument and returns its bitwise complement (one's complement). This function is part of PostgreSQL's built-in integer bitwise operations and inverts all bits in the integer value - changing all 0 bits to 1 and all 1 bits to 0.

The function uses PostgreSQL's function call convention with PG_FUNCTION_ARGS macro to access arguments and PG_RETURN_INT32 macro to return the result in the proper Datum format.

## Parameters / Member Variables
- `arg1` (PG_GETARG_INT32(0)): The 32-bit signed integer value to be bitwise complemented

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 (macro for extracting int32 arguments)
  - PG_RETURN_INT32 (macro for returning int32 result)
- Called from (representative examples):
  - SQL queries using bitwise NOT operations on integer values
  - Internal PostgreSQL operator evaluation system

## Notes and Other Information
- This function is located in src/backend/utils/adt/int.c:1438-1445
- Part of PostgreSQL's arithmetic and bitwise operations for the int4 data type
- The bitwise NOT operation flips all bits: ~0 becomes -1, ~(-1) becomes 0
- For any integer x, the result is -(x+1) due to two's complement representation
- Result follows standard C bitwise complement semantics for 32-bit signed integers
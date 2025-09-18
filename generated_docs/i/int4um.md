# int4um

## Location
src/backend/utils/adt/int.c: 771 - 782

## Overview
A PostgreSQL function that implements unary minus (negation) operation for int4 (integer) values with overflow protection.

## Definition
```c
Datum int4um(PG_FUNCTION_ARGS)
```

## Detailed Description
This function performs unary minus operation on a 32-bit signed integer (int4). It takes a single int4 argument and returns its negation. The function includes critical overflow protection by checking if the input value is PG_INT32_MIN (the most negative 32-bit integer value, typically -2147483648). Since the positive equivalent of this value cannot be represented in a 32-bit signed integer, attempting to negate it would cause integer overflow. When this condition is detected, the function raises a numeric value out of range error instead of returning an incorrect result.

The function follows PostgreSQL's standard function interface pattern, using PG_FUNCTION_ARGS for parameter access and PG_RETURN_INT32 for the return value.

## Parameters / Member Variables
- `PG_GETARG_INT32(0)`: The int4 value to negate

## Dependencies
- Functions called/Symbols referenced:
  - PG_INT32_MIN (constant for overflow detection)
  - ereport (error reporting)
  - ERROR (error level)
  - errcode (error code specification)
  - errmsg (error message)
  - ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:771-782
- Part of PostgreSQL's arithmetic operation functions for integer types
- The overflow check is essential for maintaining data integrity and preventing silent wraparound errors
- This is the "um" (unary minus) variant of int4 operations, as indicated by the naming convention
- The function demonstrates PostgreSQL's commitment to safe arithmetic operations with proper error handling
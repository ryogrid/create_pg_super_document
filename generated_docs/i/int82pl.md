# int82pl

## Location
src/backend/utils/adt/int8.c: 1032 - 1045

## Overview
Adds a 64-bit integer (int8) and a 16-bit integer (int2) and returns a 64-bit integer result with overflow checking.

## Definition
```c
Datum int82pl(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int82pl` function performs addition between a 64-bit integer and a 16-bit integer. It takes two arguments through PostgreSQLs function calling convention: the first argument is a 64-bit integer (int8) and the second is a 16-bit integer (int2). The function converts the 16-bit integer to 64-bit and performs the addition with overflow detection. If overflow occurs, it raises an error with the message "bigint out of range".

## Parameters / Member Variables
- `arg1`: 64-bit integer (int8) operand retrieved from function arguments
- `arg2`: 16-bit integer (int2) operand retrieved from function arguments  
- `result`: 64-bit integer to store the addition result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Retrieves the first 64-bit integer argument
  - PG_GETARG_INT16: Retrieves the second 16-bit integer argument
  - pg_add_s64_overflow: Performs 64-bit addition with overflow detection
  - ereport: Reports error when overflow occurs
  - PG_RETURN_INT64: Returns the 64-bit result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQLs arithmetic operations for mixed integer types
- Uses PostgreSQLs overflow-safe addition function to prevent silent overflow
- Follows PostgreSQLs standard function calling convention using PG_FUNCTION_ARGS
- The smaller 16-bit operand is cast to 64-bit before the operation
- Located in src/backend/utils/adt/int8.c at lines 1032-1045
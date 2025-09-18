# int82mi

## Location
[src/backend/utils/adt/int8.c:1046-1059](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1046-L1059)

## Overview
Subtracts a 16-bit integer (int2) from a 64-bit integer (int8) and returns a 64-bit integer result with overflow checking.

## Definition
```c
Datum int82mi(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int82mi` function performs subtraction between a 64-bit integer and a 16-bit integer. It takes two arguments through PostgreSQLs function calling convention: the first argument is a 64-bit integer (int8) minuend and the second is a 16-bit integer (int2) subtrahend. The function converts the 16-bit integer to 64-bit and performs the subtraction with overflow detection. If overflow occurs, it raises an error with the message "bigint out of range".

## Parameters / Member Variables
- `arg1`: 64-bit integer (int8) minuend retrieved from function arguments
- `arg2`: 16-bit integer (int2) subtrahend retrieved from function arguments  
- `result`: 64-bit integer to store the subtraction result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64: Retrieves the first 64-bit integer argument
  - PG_GETARG_INT16: Retrieves the second 16-bit integer argument
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md): Performs 64-bit subtraction with overflow detection
  - ereport: Reports error when overflow occurs
  - PG_RETURN_INT64: Returns the 64-bit result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQLs arithmetic operations for mixed integer types
- Uses PostgreSQLs overflow-safe subtraction function to prevent silent overflow
- Follows PostgreSQLs standard function calling convention using PG_FUNCTION_ARGS
- The smaller 16-bit operand is cast to 64-bit before the operation
- Located in src/backend/utils/adt/int8.c at lines 1046-1059
# int84mi

## Location
[src/backend/utils/adt/int8.c:904-917](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L904-L917)

## Overview
The int84mi function performs subtraction of a 32-bit integer from a 64-bit integer, returning the result as a 64-bit integer with overflow detection.

## Definition
```c
Datum int84mi(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the subtraction operation for PostgreSQL's bigint (int8) and integer (int4) data types. It takes a 64-bit integer as the first argument and subtracts a 32-bit integer (cast to 64-bit) from it. The function includes overflow detection using PostgreSQL's safe arithmetic functions to prevent integer overflow errors. If an overflow is detected, it raises an error with the appropriate error code and message.

## Parameters / Member Variables
- `arg1`: 64-bit integer (bigint) - the minuend from which the subtraction is performed
- `arg2`: 32-bit integer (int) - the subtrahend that is subtracted from arg1
- `result`: 64-bit integer - stores the computed difference

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 - retrieves the first 64-bit integer argument
  - PG_GETARG_INT32 - retrieves the second 32-bit integer argument  
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) - performs safe 64-bit subtraction with overflow detection
  - PG_RETURN_INT64 - returns the 64-bit result
  - ereport - reports errors when overflow occurs
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's int8 (bigint) arithmetic operations module
- The function uses PostgreSQL's safe arithmetic functions to prevent silent overflow
- Error handling follows PostgreSQL's standard error reporting mechanism
- The 32-bit argument is implicitly cast to 64-bit before the subtraction operation
- Located in src/backend/utils/adt/int8.c:904-917
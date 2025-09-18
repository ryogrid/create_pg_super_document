# int48pl

## Location
[src/backend/utils/adt/int8.c:971-984](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L971-L984)

## Overview
The int48pl function performs addition of a 32-bit integer and a 64-bit integer, returning the result as a 64-bit integer with overflow detection.

## Definition
```c
Datum int48pl(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the addition operation for PostgreSQL's integer (int4) and bigint (int8) data types. It takes a 32-bit integer as the first argument and adds it to a 64-bit integer. The 32-bit argument is cast to 64-bit before the addition operation. The function includes overflow detection using PostgreSQL's safe arithmetic functions to prevent integer overflow errors. If an overflow is detected, it raises an error with the appropriate error code and message.

## Parameters / Member Variables
- `arg1`: 32-bit integer (int) - the first addend that is cast to 64-bit
- `arg2`: 64-bit integer (bigint) - the second addend
- `result`: 64-bit integer - stores the computed sum

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 - retrieves the first 32-bit integer argument
  - PG_GETARG_INT64 - retrieves the second 64-bit integer argument  
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md) - performs safe 64-bit addition with overflow detection
  - PG_RETURN_INT64 - returns the 64-bit result
  - ereport - reports errors when overflow occurs
- Called from (representative examples):
  - No direct callers found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's int8 (bigint) arithmetic operations module
- The function uses PostgreSQL's safe arithmetic functions to prevent silent overflow
- Error handling follows PostgreSQL's standard error reporting mechanism
- The 32-bit argument is implicitly cast to 64-bit before the addition operation
- This is the complement to int84pl (bigint + int), handling int + bigint operations
- Located in src/backend/utils/adt/int8.c:971-984
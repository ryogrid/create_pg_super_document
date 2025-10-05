# int48mi

## Location
[src/backend/utils/adt/int8.c:985-998](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L985-L998)

## Overview
The int48mi function performs subtraction of a 64-bit integer from a 32-bit integer, returning the result as a 64-bit integer with overflow detection.

## Definition
```c
Datum int48mi(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the subtraction operation for PostgreSQL's integer (int4) and bigint (int8) data types. It takes a 32-bit integer as the first argument (minuend) and subtracts a 64-bit integer from it. The 32-bit argument is cast to 64-bit before the subtraction operation. The function includes overflow detection using PostgreSQL's safe arithmetic functions to prevent integer overflow errors. If an overflow is detected, it raises an error with the appropriate error code and message.

## Parameters / Member Variables
- `arg1`: 32-bit integer (int) - the minuend that is cast to 64-bit
- `arg2`: 64-bit integer (bigint) - the subtrahend to be subtracted from arg1
- `result`: 64-bit integer - stores the computed difference

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32 - retrieves the first 32-bit integer argument
  - PG_GETARG_INT64 - retrieves the second 64-bit integer argument  
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
- This is the complement to int84mi (bigint - int), handling int - bigint operations
- Located in src/backend/utils/adt/int8.c:985-998

## Simplified Source

```c
Datum int48mi(PG_FUNCTION_ARGS) {
    int32 arg1 = PG_GETARG_INT32(0);  // Get first 32-bit argument
    int64 arg2 = PG_GETARG_INT64(1);  // Get second 64-bit argument
    int64 result;

    // Perform subtraction with overflow check (cast arg1 to 64-bit)
    if (pg_sub_s64_overflow((int64) arg1, arg2, &result))
        ereport(ERROR, (errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE),
                       errmsg("bigint out of range")));

    PG_RETURN_INT64(result);
}
```
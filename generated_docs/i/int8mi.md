# int8mi

## Location
[src/backend/utils/adt/int8.c:476-489](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L476-L489)

## Overview
The int8mi function implements subtraction operation for two 64-bit signed integers (bigint) in PostgreSQL, with overflow detection and error handling.

## Definition

```c
Datum
int8mi(PG_FUNCTION_ARGS)
```
## Detailed Description
This function performs subtraction of two 64-bit signed integer arguments (arg1 - arg2). It extracts both input arguments using PostgreSQL's function argument macros, performs overflow-safe subtraction using the pg_sub_s64_overflow utility function, and returns the result. If overflow is detected during the subtraction operation, the function reports an error with an appropriate error code. This function is part of PostgreSQL's arithmetic operators for the bigint data type and ensures mathematical correctness by preventing silent overflow conditions that could occur when subtracting large negative numbers from large positive numbers.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function argument mechanism where:

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 arguments)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (overflow-safe subtraction function)
  - PG_RETURN_INT64 (macro for returning int64 result)
  - ereport (error reporting function)
- Called from: 
  - This function is typically invoked through PostgreSQL's function call mechanism for bigint subtraction operations

## Notes and Other Information
- Uses pg_sub_s64_overflow for safe arithmetic that detects overflow conditions before they occur
- Subtraction overflow can occur when subtracting a large negative number from a large positive number
- Error handling follows PostgreSQL conventions by using ereport with ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Located in src/backend/utils/adt/int8.c:476-489
- This is a binary arithmetic operator that requires two int64 operands
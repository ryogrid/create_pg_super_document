# int84pl

## Location
[src/backend/utils/adt/int8.c:890-903](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L890-L903)

## Overview
Adds a 64-bit signed integer and a 32-bit signed integer with overflow detection, returning the sum as a 64-bit integer.

## Definition
Datum int84pl(PG_FUNCTION_ARGS)

## Detailed Description
int84pl performs addition between a bigint (64-bit) and an integer (32-bit) value, automatically promoting the 32-bit integer to 64-bit before the operation. The function includes overflow detection using pg_add_s64_overflow to ensure that the result fits within the range of a 64-bit signed integer. If overflow occurs, it reports an error with the message "bigint out of range". This function handles mixed-precision addition operations common in SQL expressions.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Contains two arguments for the addition operation
  - arg1: First operand as 64-bit signed integer retrieved via PG_GETARG_INT64(0)
  - arg2: Second operand as 32-bit signed integer retrieved via PG_GETARG_INT32(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64
  - PG_GETARG_INT32
  - [pg_add_s64_overflow](../p/pg_add_s64_overflow.md)
  - PG_RETURN_INT64
  - ereport
  - [errcode](../e/errcode.md)
  - [errmsg](../e/errmsg.md)
  - ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
This function is part of PostgreSQL's mixed-type arithmetic operations, allowing addition between bigint and integer types. The overflow checking ensures mathematical correctness and prevents silent wraparound that could lead to incorrect results. The function is defined in src/backend/utils/adt/int8.c:890-903.
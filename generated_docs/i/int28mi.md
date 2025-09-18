# int28mi

## Location
[src/backend/utils/adt/int8.c:1127-1140](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int8.c#L1127-L1140)

## Overview
Subtracts a 64-bit integer (bigint) from a 16-bit integer (smallint) and returns the result as a 64-bit integer.

## Definition


## Detailed Description
This function implements subtraction of an 8-byte integer from a 2-byte integer in PostgreSQL. The operation computes arg1 - arg2 where arg1 is a 16-bit integer and arg2 is a 64-bit integer. The 16-bit argument is implicitly converted to 64-bit before the subtraction operation.

The function performs safe subtraction using PostgreSQL's overflow-checking arithmetic functions to detect underflow conditions and reports an error if the result would exceed the range of a 64-bit signed integer.

## Parameters / Member Variables
-  (int16): The minuend (16-bit integer from first function argument)
-  (int64): The subtrahend (64-bit integer from second function argument)  
-  (int64): The computed difference

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (extracts 16-bit argument)
  - PG_GETARG_INT64 (extracts 64-bit argument)
  - [pg_sub_s64_overflow](../p/pg_sub_s64_overflow.md) (safe 64-bit subtraction with overflow detection)
  - PG_RETURN_INT64 (returns 64-bit result)
  - ereport (error reporting)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is defined in src/backend/utils/adt/int8.c:1127-1140
- Uses PostgreSQL's overflow-safe arithmetic functions to prevent integer underflow
- The function name follows PostgreSQL's convention where 'int2' refers to 16-bit integers, 'int8' refers to 64-bit integers, and 'mi' indicates minus/subtraction
- Automatically promotes the smaller integer type to match the larger one before computation  
- Reports NUMERIC_VALUE_OUT_OF_RANGE error when overflow/underflow occurs
- Note the order of operands: this computes (smallint - bigint), not (bigint - smallint)
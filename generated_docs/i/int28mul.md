# int28mul

## Location
src/backend/utils/adt/int8.c: 1141 - 1154

## Overview
Multiplies a 16-bit integer (smallint) by a 64-bit integer (bigint) and returns the result as a 64-bit integer.

## Definition


## Detailed Description
This function implements multiplication of a 2-byte integer with an 8-byte integer in PostgreSQL. The function performs safe multiplication by using PostgreSQL's overflow-checking arithmetic functions. The 16-bit argument is implicitly converted to 64-bit before the multiplication operation to ensure proper type compatibility.

The function uses PostgreSQL's pg_mul_s64_overflow utility to detect overflow conditions during multiplication and reports an error if the result would exceed the range of a 64-bit signed integer.

## Parameters / Member Variables
-  (int16): The first multiplicand (16-bit integer from first function argument)
-  (int64): The second multiplicand (64-bit integer from second function argument)  
-  (int64): The computed product

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 (extracts 16-bit argument)
  - PG_GETARG_INT64 (extracts 64-bit argument)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (safe 64-bit multiplication with overflow detection)
  - PG_RETURN_INT64 (returns 64-bit result)
  - ereport (error reporting)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function is defined in src/backend/utils/adt/int8.c:1141-1154
- Uses PostgreSQL's overflow-safe arithmetic functions to prevent integer overflow
- The function name follows PostgreSQL's convention where 'int2' refers to 16-bit integers, 'int8' refers to 64-bit integers, and 'mul' indicates multiplication
- Automatically promotes the smaller integer type to match the larger one before computation
- Reports NUMERIC_VALUE_OUT_OF_RANGE error when overflow occurs
- Multiplication is commutative, so the order of operands doesn't affect the mathematical result
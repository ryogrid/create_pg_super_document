# int8mul

## Location
src/backend/utils/adt/int8.c: 490 - 503

## Overview
The int8mul function implements multiplication operation for two 64-bit signed integers (bigint) in PostgreSQL, with overflow detection and error handling.

## Definition


## Detailed Description
This function performs multiplication of two 64-bit signed integer arguments. It extracts both input arguments using PostgreSQL's function argument macros, performs overflow-safe multiplication using the pg_mul_s64_overflow utility function, and returns the result. If overflow is detected during the multiplication operation, the function reports an error with an appropriate error code. This function is part of PostgreSQL's arithmetic operators for the bigint data type and ensures mathematical correctness by preventing silent overflow conditions that are particularly common in multiplication operations with large numbers.

## Parameters / Member Variables
- The function uses PostgreSQL's standard function argument mechanism where:
  - First argument (multiplicand) accessed via PG_GETARG_INT64(0) 
  - Second argument (multiplier) accessed via PG_GETARG_INT64(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting int64 arguments)
  - [pg_mul_s64_overflow](../p/pg_mul_s64_overflow.md) (overflow-safe multiplication function)
  - PG_RETURN_INT64 (macro for returning int64 result)
  - ereport (error reporting function)
- Called from (representative examples):
  - [int4_cash](int4_cash.md) (currency conversion function)
  - [int8_cash](int8_cash.md) (currency conversion function) 
  - [int8_to_char](int8_to_char.md) (formatting function)

## Notes and Other Information
- Uses pg_mul_s64_overflow for safe arithmetic that detects overflow conditions before they occur
- Multiplication overflow is particularly easy to trigger with moderately large numbers due to the exponential nature of the operation
- Error handling follows PostgreSQL conventions by using ereport with ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE
- Located in src/backend/utils/adt/int8.c:490-503
- This is a binary arithmetic operator that requires two int64 operands
- Unlike the other int8 arithmetic functions, this one has several known callers in the codebase, particularly in currency and formatting operations
# int24mi

## Location
src/backend/utils/adt/int.c: 1002 - 1015

## Overview
 is a PostgreSQL function that performs subtraction between a 16-bit integer (int2/smallint) and a 32-bit integer (int4/integer), returning a 32-bit integer result with overflow checking.

## Definition


## Detailed Description
This function implements the subtraction operator for mixed-precision integer arithmetic in PostgreSQL's type system. It takes a 16-bit integer as the first argument (minuend) and a 32-bit integer as the second argument (subtrahend), performs safe subtraction with overflow detection, and returns the result as a 32-bit integer. The function uses PostgreSQL's safe arithmetic functions to prevent integer underflow/overflow, throwing an error if the result would exceed the range of a 32-bit signed integer.

## Parameters / Member Variables
- : 16-bit signed integer (int2/smallint) - the minuend (value being subtracted from)
- : 32-bit signed integer (int4/integer) - the subtrahend (value being subtracted)
- : 32-bit signed integer to store the subtraction result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16 - extracts 16-bit integer argument
  - PG_GETARG_INT32 - extracts 32-bit integer argument  
  - [pg_sub_s32_overflow](../p/pg_sub_s32_overflow.md) - performs safe 32-bit integer subtraction with overflow detection
  - PG_RETURN_INT32 - returns 32-bit integer result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int.c:1002-1015
- Part of PostgreSQL's arithmetic operator system for mixed integer types
- Throws ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE error if subtraction would overflow/underflow
- The function follows PostgreSQL's function call convention using PG_FUNCTION_ARGS
- Promotes the 16-bit argument to 32-bit before performing the subtraction operation
- Companion function to int24pl for mixed-precision arithmetic operations
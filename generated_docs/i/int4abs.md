# int4abs

## Location
[src/backend/utils/adt/int.c:1191-1204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/int.c#L1191-L1204)

## Overview
Computes the absolute value of a 32-bit integer (int4) with overflow protection.

## Definition


## Detailed Description
The  function implements PostgreSQL's absolute value operation for 32-bit integers. It safely computes the absolute value while protecting against integer overflow that would occur when attempting to negate the minimum value of a signed 32-bit integer (). When the input is the minimum possible 32-bit signed integer value, the function raises an error since the absolute value cannot be represented in the same data type.

## Parameters / Member Variables
- Input parameter (accessed via ): The 32-bit integer whose absolute value is to be computed

## Dependencies
- Functions called/Symbols referenced:
  - : Constant representing the minimum value for a 32-bit signed integer
  - : PostgreSQL macro to extract int32 argument
  - : PostgreSQL macro to return int32 result
  - : PostgreSQL error reporting function
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function includes overflow protection by checking if the input equals  (-2,147,483,648), which cannot be negated without overflow in 32-bit signed arithmetic
- Uses PostgreSQL's function calling convention with  and return macros
- Part of PostgreSQL's integer arithmetic functions located in 
- The  macro is used for branch prediction optimization on the overflow check
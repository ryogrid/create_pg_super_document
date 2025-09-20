# cash_mul_int4

## Location
[src/backend/utils/adt/cash.c:851-863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/cash.c#L851-L863)

## Overview
A PostgreSQL function that multiplies a Cash value by a 32-bit signed integer, providing safe arithmetic operations for monetary calculations.

## Definition

```c
Datum
cash_mul_int4(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that performs multiplication of a Cash data type by a 32-bit signed integer (int4). It serves as a wrapper around the internal  helper function, promoting the int4 parameter to int64 for consistent internal arithmetic handling. This function is part of PostgreSQL's monetary data type system and ensures safe multiplication operations without integer overflow issues.

The function follows PostgreSQL's standard function calling convention, using  to access arguments and  to return the result.

## Parameters / Member Variables
-  (Cash): The monetary value to be multiplied (first argument)
-  (int32): The 32-bit signed integer multiplier (second argument)

## Dependencies
- Functions called/Symbols referenced:
  - : Extracts Cash value from function arguments
  - : Extracts int32 value from function arguments  
  - : Internal helper function for safe Cash multiplication
  - : Returns Cash value as function result
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL operator interface)

## Notes and Other Information
- This function delegates the actual multiplication logic to  by promoting the int4 parameter to int64
- Part of PostgreSQL's type system for safe monetary arithmetic operations
- Follows the standard PostgreSQL function interface pattern for built-in functions
- The function is designed to prevent overflow issues by using the 64-bit multiplication helper internally
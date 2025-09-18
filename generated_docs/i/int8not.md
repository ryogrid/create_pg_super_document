# int8not

## Location
src/backend/utils/adt/int8.c: 1211 - 1218

## Overview
The int8not function performs bitwise NOT operation on a single 64-bit integer, returning the bitwise complement as a 64-bit integer.

## Definition
```c
Datum int8not(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL bitwise NOT operator (~) for the BIGINT data type (int8). It extracts a single 64-bit integer argument from the function call context, performs a bitwise NOT operation (bitwise complement) on it, and returns the result. Unlike the other bitwise operations in this family, this is a unary operator that takes only one operand. The function follows the standard PostgreSQL function interface pattern using the Datum return type and PG_FUNCTION_ARGS parameter convention.

## Parameters / Member Variables
- The function uses the standard PostgreSQL function interface where the argument is accessed through PG_GETARG_INT64() macro:
  - First argument: 64-bit integer operand (arg1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting 64-bit integer arguments)
  - PG_RETURN_INT64 (macro for returning 64-bit integer result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c:1211-1218
- Part of a family of bitwise operations for 64-bit integers including int8and, int8or, int8xor, int8shl, and int8shr
- Implements the PostgreSQL bitwise NOT operator (~) for BIGINT data type
- This is a unary operator, unlike the other binary bitwise operations in this family
- Uses standard PostgreSQL V1 function call convention
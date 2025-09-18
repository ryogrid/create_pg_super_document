# int8and

## Location
src/backend/utils/adt/int8.c: 1184 - 1192

## Overview
The int8and function performs bitwise AND operation on two 64-bit integers, returning the result as a 64-bit integer.

## Definition


## Detailed Description
This function implements the PostgreSQL bitwise AND operator (&) for the BIGINT data type (int8). It extracts two 64-bit integer arguments from the function call context, performs a bitwise AND operation on them, and returns the result. The function is part of PostgreSQL's binary arithmetic operations for 64-bit integers and follows the standard PostgreSQL function interface pattern using the Datum return type and PG_FUNCTION_ARGS parameter convention.

## Parameters / Member Variables
- The function uses the standard PostgreSQL function interface where arguments are accessed through PG_GETARG_INT64() macros:
  - First argument: 64-bit integer operand (arg1)
  - Second argument: 64-bit integer operand (arg2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting 64-bit integer arguments)
  - PG_RETURN_INT64 (macro for returning 64-bit integer result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c:1184-1192
- Part of a family of bitwise operations for 64-bit integers including int8or, int8xor, int8not, int8shl, and int8shr
- Implements the PostgreSQL bitwise AND operator (&) for BIGINT data type
- Uses standard PostgreSQL V1 function call convention
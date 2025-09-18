# int8shl

## Location
src/backend/utils/adt/int8.c: 1219 - 1227

## Overview
The int8shl function performs bitwise left shift operation on a 64-bit integer by a specified number of positions, returning the result as a 64-bit integer.

## Definition
```c
Datum int8shl(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL bitwise left shift operator (<<) for the BIGINT data type (int8). It extracts a 64-bit integer argument (the value to be shifted) and a 32-bit integer argument (the number of positions to shift) from the function call context, performs a bitwise left shift operation, and returns the result. The function is part of PostgreSQL's binary arithmetic operations for 64-bit integers and follows the standard PostgreSQL function interface pattern using the Datum return type and PG_FUNCTION_ARGS parameter convention.

## Parameters / Member Variables
- The function uses the standard PostgreSQL function interface where arguments are accessed through PG_GETARG_* macros:
  - First argument: 64-bit integer operand to be shifted (arg1) - accessed via PG_GETARG_INT64(0)
  - Second argument: 32-bit integer shift count (arg2) - accessed via PG_GETARG_INT32(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT64 (macro for extracting 64-bit integer arguments)
  - PG_GETARG_INT32 (macro for extracting 32-bit integer arguments)  
  - PG_RETURN_INT64 (macro for returning 64-bit integer result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/int8.c:1219-1227
- Part of a family of bitwise operations for 64-bit integers including int8and, int8or, int8xor, int8not, and int8shr
- Implements the PostgreSQL bitwise left shift operator (<<) for BIGINT data type
- Unlike other bitwise operations in this family, this function takes arguments of different types: int64 and int32
- Uses standard PostgreSQL V1 function call convention
- No bounds checking is performed on the shift count, following C language semantics
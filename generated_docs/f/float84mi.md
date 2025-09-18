# float84mi

## Location
src/backend/utils/adt/float.c: 3828 - 3836

## Overview
The float84mi function performs subtraction between a float8 (double precision) and a float4 (single precision) number, returning the result as a float8.

## Definition
```c
Datum float84mi(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the subtraction operation for mixed-precision floating-point numbers in PostgreSQL's type system. It takes a float8 (8-byte double precision) value as the first operand and a float4 (4-byte single precision) value as the second operand. The function promotes the float4 value to float8 precision and then delegates to the float8_mi function to perform the actual subtraction operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments and context
  - `arg1` (float8): The first operand - a double precision floating-point number
  - `arg2` (float4): The second operand - a single precision floating-point number

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Macro to extract float8 argument from function arguments
  - PG_GETARG_FLOAT4: Macro to extract float4 argument from function arguments  
  - [float8_mi](float8_mi.md): Function that performs double precision floating-point subtraction
  - PG_RETURN_FLOAT8: Macro to return a float8 result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operator framework for mixed-precision floating-point operations
- The result is always returned as float8 (double precision) to preserve precision
- The function follows PostgreSQL's naming convention: float84mi indicates float8 - float4 subtraction
- This is the reverse operand order compared to float48mi - float8 comes first, then float4
- Located in src/backend/utils/adt/float.c:3828-3836
- Part of a family of float84 functions including float84pl, float84mul, and float84div
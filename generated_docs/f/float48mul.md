# float48mul

## Location
[src/backend/utils/adt/float.c:3795-3803](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3795-L3803)

## Overview
The float48mul function performs multiplication between a float4 (single precision) and a float8 (double precision) number, returning the result as a float8.

## Definition
```c
Datum float48mul(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the multiplication operation for mixed-precision floating-point numbers in PostgreSQL's type system. It takes a float4 (4-byte single precision) value as the first operand and a float8 (8-byte double precision) value as the second operand. The function promotes the float4 value to float8 precision and then delegates to the float8_mul function to perform the actual multiplication operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments and context
  - `arg1` (float4): The first operand - a single precision floating-point number
  - `arg2` (float8): The second operand - a double precision floating-point number

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4: Macro to extract float4 argument from function arguments
  - PG_GETARG_FLOAT8: Macro to extract float8 argument from function arguments  
  - [float8_mul](float8_mul.md): Function that performs double precision floating-point multiplication
  - PG_RETURN_FLOAT8: Macro to return a float8 result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operator framework for mixed-precision floating-point operations
- The result is always returned as float8 (double precision) to preserve precision
- The function follows PostgreSQL's naming convention: float48mul indicates float4 * float8 multiplication
- Located in src/backend/utils/adt/float.c:3795-3803
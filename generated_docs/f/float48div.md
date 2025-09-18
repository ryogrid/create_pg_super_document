# float48div

## Location
[src/backend/utils/adt/float.c:3804-3818](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3804-L3818)

## Overview
The float48div function performs division between a float4 (single precision) and a float8 (double precision) number, returning the result as a float8.

## Definition
```c
Datum float48div(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the division operation for mixed-precision floating-point numbers in PostgreSQL's type system. It takes a float4 (4-byte single precision) value as the dividend and a float8 (8-byte double precision) value as the divisor. The function promotes the float4 value to float8 precision and then delegates to the float8_div function to perform the actual division operation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments and context
  - `arg1` (float4): The dividend - a single precision floating-point number
  - `arg2` (float8): The divisor - a double precision floating-point number

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4: Macro to extract float4 argument from function arguments
  - PG_GETARG_FLOAT8: Macro to extract float8 argument from function arguments  
  - [float8_div](float8_div.md): Function that performs double precision floating-point division
  - PG_RETURN_FLOAT8: Macro to return a float8 result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's arithmetic operator framework for mixed-precision floating-point operations
- The result is always returned as float8 (double precision) to preserve precision
- Division by zero handling is delegated to the float8_div function
- The function follows PostgreSQL's naming convention: float48div indicates float4 / float8 division
- Located in src/backend/utils/adt/float.c:3804-3818
# float84div

## Location
[src/backend/utils/adt/float.c:3846-3863](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3846-L3863)

## Overview
Divides a float8 (double precision) value by a float4 (single precision) value, returning the result as a float8.

## Definition
```c
Datum float84div(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the division operator for mixed-precision floating-point arithmetic in PostgreSQL. It takes a float8 (double precision) value as the dividend (first argument) and a float4 (single precision) value as the divisor (second argument). The function promotes the float4 divisor to float8 precision and then performs the division using the internal float8_div function, which includes proper handling of division by zero and other edge cases while maintaining double precision accuracy.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `arg1`: float8 (double precision) dividend
  - `arg2`: float4 (single precision) divisor, automatically promoted to float8

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
  - PG_GETARG_FLOAT4: Extracts float4 argument from function call context
  - [float8_div](float8_div.md): Performs the actual float8 division with error handling
  - PG_RETURN_FLOAT8: Returns the float8 result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:3846-3863
- This function is part of PostgreSQL's type system for handling mixed-precision arithmetic
- The float4 divisor is implicitly cast to float8 before division to maintain precision
- Division by zero and other floating-point exceptions are handled by the underlying float8_div function
- Returns a Datum containing the float8 result
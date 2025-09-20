# float84mul

## Location
[src/backend/utils/adt/float.c:3837-3845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L3837-L3845)

## Overview
Multiplies a float8 (double precision) value by a float4 (single precision) value, returning the result as a float8.

## Definition

```c
Datum
float84mul(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the multiplication operator for mixed-precision floating-point arithmetic in PostgreSQL. It takes a float8 (double precision) value as the first argument and a float4 (single precision) value as the second argument. The function promotes the float4 argument to float8 precision and then performs the multiplication using the internal float8_mul function, ensuring the result maintains double precision accuracy.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : float8 (double precision) multiplicand
  - : float4 (single precision) multiplier, automatically promoted to float8

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
  - PG_GETARG_FLOAT4: Extracts float4 argument from function call context
  - [float8_mul](float8_mul.md): Performs the actual float8 multiplication
  - PG_RETURN_FLOAT8: Returns the float8 result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:3837-3845
- This function is part of PostgreSQL's type system for handling mixed-precision arithmetic
- The float4 argument is implicitly cast to float8 before multiplication to maintain precision
- Returns a Datum containing the float8 result
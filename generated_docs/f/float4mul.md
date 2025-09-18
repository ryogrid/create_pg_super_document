# float4mul

## Location
[src/backend/utils/adt/float.c:739-747](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L739-L747)

## Overview
PostgreSQL SQL-callable function that performs multiplication of two single-precision floating-point numbers (float4), implementing the '*' operator for the float4 data type.

## Definition
```c
Datum float4mul(PG_FUNCTION_ARGS)
```

## Detailed Description
The `float4mul` function is a PostgreSQL fmgr-compatible function that multiplies two float4 values. It extracts two float4 arguments from the function call context using PostgreSQL's function manager macros, delegates the actual arithmetic operation to the inline helper function `float4_mul`, and returns the result as a PostgreSQL Datum. This function provides both overflow and underflow detection - if the multiplication of two finite values results in infinity, it raises an overflow error, and if the multiplication of two non-zero finite values results in zero, it raises an underflow error.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function manager argument structure containing:
  - arg1 (extracted as float4): First multiplicand
  - arg2 (extracted as float4): Second multiplicand

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT4 (macro to extract float4 arguments)
  - [float4_mul](float4_mul.md) (inline helper function that performs the actual multiplication with overflow and underflow checking)
  - PG_RETURN_FLOAT4 (macro to return float4 result as Datum)
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's function manager system for the '*' operator)

## Notes and Other Information
- This is part of PostgreSQL's arithmetic operator implementation for the float4 data type
- The function uses PostgreSQL's fmgr (function manager) calling convention
- Both overflow and underflow detection are performed by the underlying float4_mul function
- Located in src/backend/utils/adt/float.c alongside other float4 arithmetic operators
- The function signature follows PostgreSQL's standard pattern for SQL-callable functions
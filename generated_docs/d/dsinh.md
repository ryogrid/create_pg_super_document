# dsinh

## Location
[src/backend/utils/adt/float.c:2591-2619](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2591-L2619)

## Overview
The  function computes the hyperbolic sine of a given floating-point argument, with robust error handling for overflow conditions.

## Definition

```c
Datum
dsinh(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the hyperbolic sine mathematical function (sinh) for PostgreSQL. It uses the standard C library  function to compute the hyperbolic sine and includes comprehensive error handling for overflow conditions. When an overflow occurs (detected via ERANGE errno), the function returns appropriate positive or negative infinity values based on the sign of the input argument. The hyperbolic sine function is defined mathematically as sinh(x) = (e^x - e^(-x))/2, and it's commonly used in mathematical modeling, physics calculations, and various engineering applications.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input value for which to compute the hyperbolic sine (float8 type extracted via PG_GETARG_FLOAT8)
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call
  - sinh: Standard C library hyperbolic sine function
  - [get_float8_infinity](../g/get_float8_infinity.md): Returns positive infinity for float8 type
- Called from: No direct references found in the codebase

## Notes and Other Information
- Part of the hyperbolic functions section in the float.c file
- Handles overflow by returning signed infinity based on input argument sign
- Uses errno to detect ERANGE errors from the underlying sinh() function
- For negative inputs that overflow, returns negative infinity
- For positive inputs that overflow, returns positive infinity
- Located in src/backend/utils/adt/float.c:2591-2619
- Mathematically: sinh(x) = (e^x - e^(-x))/2
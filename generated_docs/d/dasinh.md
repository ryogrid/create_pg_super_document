# dasinh

## Location
[src/backend/utils/adt/float.c:2665-2681](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2665-L2681)

## Overview
The dasinh function computes the inverse hyperbolic sine of a floating-point number, providing PostgreSQL integration for mathematical operations.

## Definition
Datum dasinh(PG_FUNCTION_ARGS)

## Detailed Description
The dasinh function is a PostgreSQL wrapper around the standard C library asinh() function that calculates the inverse hyperbolic sine (also known as area hyperbolic sine) of a given floating-point argument. The inverse hyperbolic sine function is mathematically stable and never overflows for finite inputs, so no errno checking is required. The function extracts a float8 (double precision) argument from the PostgreSQL function call context and computes the inverse hyperbolic sine using the system asinh() function. This is one of the simpler mathematical wrapper functions in PostgreSQL due to the mathematical properties of asinh.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function call context containing the input argument
- arg1: The input float8 value for which to compute the inverse hyperbolic sine
- result: The computed inverse hyperbolic sine result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The asinh function never mathematically overflows for finite inputs, making it very stable
- No special error handling is required beyond the basic PostgreSQL function interface
- The function is part of PostgreSQL mathematical function library in src/backend/utils/adt/float.c
- Located at src/backend/utils/adt/float.c:2665-2681
- This is one of the simplest mathematical wrapper functions due to asinh stability

## Simplified Source

```c
Datum dasinh(PG_FUNCTION_ARGS) {
    // Calculate inverse hyperbolic sine (asinh never overflows)
    double input = PG_GETARG_FLOAT8(0);
    double result = asinh(input);

    PG_RETURN_FLOAT8(result);
}
```
# dtanh

## Location
[src/backend/utils/adt/float.c:2645-2664](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2645-L2664)

## Overview
The dtanh function computes the hyperbolic tangent of a floating-point number, providing PostgreSQL-specific error handling for mathematical operations.

## Definition
Datum dtanh(PG_FUNCTION_ARGS)

## Detailed Description
The dtanh function is a PostgreSQL wrapper around the standard C library tanh() function that calculates the hyperbolic tangent of a given floating-point argument. Unlike other hyperbolic functions, tanh never overflows due to its mathematical properties (the result is always bounded between -1 and 1), so errno checking is not required. However, the function still includes a safety check for infinite results and integrates with PostgreSQL function call interface. The function extracts a float8 (double precision) argument from the PostgreSQL function call context and computes the hyperbolic tangent using the system tanh() function.

## Parameters / Member Variables
- PG_FUNCTION_ARGS: Standard PostgreSQL function call context containing the input argument
- arg1: The input float8 value for which to compute the hyperbolic tangent
- result: The computed hyperbolic tangent result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
  - isinf: Checks if the result is infinite
  - [float_overflow_error](../f/float_overflow_error.md): Handles overflow error conditions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Unlike cosh and sinh, tanh never mathematically overflows due to its bounded range (-1, 1)
- The function includes a defensive check for infinite results as a safety measure
- The function is part of PostgreSQL mathematical function library in src/backend/utils/adt/float.c
- Located at src/backend/utils/adt/float.c:2645-2664
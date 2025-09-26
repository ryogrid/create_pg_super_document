# dcosh

## Location
[src/backend/utils/adt/float.c:2620-2644](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L2620-L2644)

## Overview
The dcosh function computes the hyperbolic cosine of a floating-point number, handling overflow conditions and providing PostgreSQL-specific error handling for mathematical operations.

## Definition

```c
Datum
dcosh(PG_FUNCTION_ARGS)
```
## Detailed Description
The dcosh function is a PostgreSQL wrapper around the standard C library cosh() function that calculates the hyperbolic cosine of a given floating-point argument. It provides robust error handling for overflow conditions and integrates with PostgreSQL's function call interface. The function extracts a float8 (double precision) argument from the PostgreSQL function call context, computes the hyperbolic cosine using the system's cosh() function, and handles potential mathematical errors such as overflow. When an overflow occurs (indicated by ERANGE errno), the function returns positive infinity since cosh is always positive. The function also checks for underflow conditions where the result might be zero.

## Parameters / Member Variables
- : Standard PostgreSQL function call context containing the input argument
- : The input float8 value for which to compute the hyperbolic cosine
- : The computed hyperbolic cosine result

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8: Extracts float8 argument from function call context
  - [get_float8_infinity](../g/get_float8_infinity.md): Returns positive infinity representation
  - [float_underflow_error](../f/float_underflow_error.md): Handles underflow error conditions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function handles overflow by setting errno to ERANGE and returns positive infinity
- Since cosh is always positive, overflow always results in positive infinity
- Underflow conditions (result == 0.0) trigger a PostgreSQL-specific error
- The function is part of PostgreSQL's mathematical function library in src/backend/utils/adt/float.c
- Located at src/backend/utils/adt/float.c:2620-2644
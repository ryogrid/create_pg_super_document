# float8abs

## Location
[src/backend/utils/adt/float.c:650-661](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L650-L661)

## Overview
Computes and returns the absolute value of a double-precision floating-point number in PostgreSQL's SQL function interface.

## Definition

```c
Datum
float8abs(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the absolute value operation for PostgreSQL's float8 (double-precision floating-point) data type. It extracts a float8 value from the function arguments using PostgreSQL's function call interface, applies the standard C library  function to compute the absolute value, and returns the result as a PostgreSQL Datum. The function is part of PostgreSQL's base operations for float8 arithmetic.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input float8 value obtained via  from the first function argument
## Dependencies
- Functions called/Symbols referenced:
  -  (macro for extracting float8 argument)
  -  (C standard library function for absolute value)
  -  (macro for returning float8 result)
- Called from (representative examples):
  - No direct references found in the codebase (likely called via SQL function dispatch)

## Notes and Other Information
- Located in src/backend/utils/adt/float.c:650-661
- Part of PostgreSQL's float8 base operations suite
- Uses standard C library  function for the actual computation
- Follows PostgreSQL's standard function call convention with
- Returns result using PostgreSQL's Datum system

## Simplified Source

```c
Datum
float8abs(PG_FUNCTION_ARGS)
{
    // Extract the float8 argument
    float8 arg1 = PG_GETARG_FLOAT8(0);

    // Return absolute value using standard C library function
    PG_RETURN_FLOAT8(fabs(arg1));
}
```
# dsign

## Location
[src/backend/utils/adt/float.c:1398-1420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/float.c#L1398-L1420)

## Overview
The dsign function returns the sign of a double-precision floating-point number, returning -1 for negative values, 0 for zero, and 1 for positive values.

## Definition

```c
Datum
dsign(PG_FUNCTION_ARGS)
```
## Detailed Description
The dsign function implements the mathematical sign function for double-precision floating-point numbers (float8). It takes a single float8 argument and returns:
- 1.0 if the argument is greater than zero
- -1.0 if the argument is less than zero  
- 0.0 if the argument is equal to zero

This is a PostgreSQL SQL-callable function that follows the PostgreSQL function calling convention, accepting arguments via PG_FUNCTION_ARGS and returning a Datum value using PG_RETURN_FLOAT8.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: The input double-precision floating-point number whose sign is to be determined
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - PG_RETURN_FLOAT8 (macro to return float8 result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's floating-point arithmetic operations
- The function handles all three cases of the sign function explicitly with simple conditional logic
- Located in src/backend/utils/adt/float.c which contains various floating-point utility functions
- Follows standard PostgreSQL function conventions for SQL-callable functions

## Simplified Source

```c
Datum dsign(PG_FUNCTION_ARGS) {
    float8 arg1 = PG_GETARG_FLOAT8(0);
    float8 result;

    // Return sign: 1.0 for positive, -1.0 for negative, 0.0 for zero
    if (arg1 > 0)
        result = 1.0;
    else if (arg1 < 0)
        result = -1.0;
    else
        result = 0.0;

    PG_RETURN_FLOAT8(result);
}
```
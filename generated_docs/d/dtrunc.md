# dtrunc

## Location
src/backend/utils/adt/float.c: 1421 - 1438

## Overview
The dtrunc function performs truncation towards zero for double-precision floating-point numbers, effectively removing the fractional part while preserving the sign.

## Definition
```c
Datum dtrunc(PG_FUNCTION_ARGS)
```

## Detailed Description
The dtrunc function implements truncation towards zero for double-precision floating-point numbers (float8). The function behavior differs based on the sign of the input:
- For non-negative arguments (arg1 >= 0): returns the greatest integer less than or equal to arg1 (equivalent to floor)
- For negative arguments (arg1 < 0): returns the least integer greater than or equal to arg1 (equivalent to -floor(-arg1))

This approach ensures that the result is always closer to zero than the original value, hence "truncation towards zero."

## Parameters / Member Variables
- `arg1`: The input double-precision floating-point number to be truncated

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (macro to extract float8 argument)
  - floor (standard C math library function)
  - PG_RETURN_FLOAT8 (macro to return float8 result)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is part of PostgreSQL's floating-point arithmetic operations
- Located in src/backend/utils/adt/float.c which contains various floating-point utility functions
- The implementation uses floor() for positive values and -floor(-arg1) for negative values to achieve truncation towards zero
- Follows standard PostgreSQL function conventions for SQL-callable functions
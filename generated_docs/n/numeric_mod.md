# numeric_mod

## Location
src/backend/utils/adt/numeric.c: 3364 - 3383

## Overview
PostgreSQL function that calculates the modulo (remainder) of two numeric values, providing the standard modulo operation for the Numeric data type.

## Definition
```c
Datum numeric_mod(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the modulo operation for PostgreSQL's Numeric data type. It serves as a simple wrapper around the more comprehensive `numeric_mod_opt_error` function, providing the standard PostgreSQL function interface for modulo calculations. The function extracts two Numeric arguments and delegates the actual modulo computation to the internal implementation.

Key behaviors:
- Calculates remainder of first argument divided by second argument
- Uses PostgreSQL's function calling convention (PG_FUNCTION_ARGS)
- Delegates to numeric_mod_opt_error with NULL error handling (standard exception throwing)
- Returns result as Numeric type

## Parameters / Member Variables
- Function arguments accessed via PG_GETARG_NUMERIC():
  - Argument 0: The dividend (numerator) - the Numeric value to find remainder for
  - Argument 1: The divisor (denominator) - the Numeric value to divide by for remainder

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC, PG_RETURN_NUMERIC
  - numeric_mod_opt_error (core implementation)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function dispatch)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL as the % operator or mod() function
- Acts as a thin wrapper around the internal numeric_mod_opt_error implementation
- Error handling follows standard PostgreSQL pattern (exceptions thrown on error)
- The actual modulo logic and special case handling is implemented in numeric_mod_opt_error
- Part of PostgreSQL's comprehensive numeric arithmetic function suite
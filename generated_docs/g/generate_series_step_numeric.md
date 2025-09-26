# generate_series_step_numeric

## Location
src/backend/utils/adt/numeric.c: 1707 - 1844

## Overview
Implements a set-returning function that generates a series of numeric values between start and stop values with a configurable step size.

## Definition

```c
Datum
generate_series_step_numeric(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL set-returning function (SRF) that generates a sequence of numeric values. It supports both two-parameter (start, stop) and three-parameter (start, stop, step) variants. When only two parameters are provided, it defaults to a step of 1. The function validates that start, stop, and step values are not NaN or infinity, and that step is not zero.

The function uses PostgreSQL's SRF framework to maintain state across multiple calls, storing the current position, stop value, and step in a context structure. It handles both positive and negative step values, determining the appropriate termination condition based on the step direction.

## Parameters / Member Variables
- Parameter 0: start_num - The starting value of the series
- Parameter 1: stop_num - The ending value of the series  
- Parameter 2 (optional): step_num - The increment/decrement value (defaults to 1)
- Context structure members:
  - : Current position in the series (NumericVar)
  - : End value of the series (NumericVar)
  - : Step increment/decrement (NumericVar)

## Dependencies
- Functions called/Symbols referenced:
  - SRF_IS_FIRSTCALL - Check if this is the first call to the SRF
  - PG_GETARG_NUMERIC - Extract numeric arguments
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN - Check for special values
  - PG_NARGS - Get number of function arguments
  - init_var_from_num, set_var_from_num, set_var_from_var - NumericVar operations
  - cmp_var - Compare numeric variables
  - add_var - Add numeric variables
  - make_result - Convert NumericVar to Numeric
  - SRF_FIRSTCALL_INIT, SRF_PERCALL_SETUP - SRF framework functions
  - SRF_RETURN_NEXT, SRF_RETURN_DONE - SRF return macros
  - NumericGetDatum - Convert Numeric to Datum
- Called from:
  - generate_series_numeric (wrapper for two-parameter version)

## Notes and Other Information
- Located in src/backend/utils/adt/numeric.c:1707-1844
- Implements PostgreSQL's set-returning function protocol for maintaining state across calls
- Validates input parameters to reject NaN, infinity, and zero step values
- Handles both ascending (positive step) and descending (negative step) series
- Uses appropriate memory contexts for multi-call persistence
- Part of PostgreSQL's generate_series function family for numeric data types
- The function context persists current position and parameters between calls
# generate_series_step_int4

## Location
src/backend/utils/adt/int.c: 1509 - 1584

## Overview
Implements the core logic for generating a series of 32-bit integers with configurable start, finish, and step values as a set-returning function.

## Definition
```c
Datum generate_series_step_int4(PG_FUNCTION_ARGS)
```

## Detailed Description
The `generate_series_step_int4` function is the main implementation of PostgreSQL's `generate_series()` function for 32-bit integers. It generates a sequence of integers from a start value to a finish value, incrementing by a specified step size. The function uses PostgreSQL's Set-Returning Function (SRF) framework to return multiple rows across multiple calls. It maintains state between function calls using a function context structure, allowing it to remember the current position in the series. The function handles both positive and negative step values and includes overflow protection to prevent infinite loops.

## Parameters / Member Variables
- `start`: The 32-bit integer starting value of the series (from PG_GETARG_INT32(0))
- `finish`: The 32-bit integer ending value of the series (from PG_GETARG_INT32(1))
- `step`: The 32-bit integer step size (from PG_GETARG_INT32(2), defaults to 1 if not provided)
- `funcctx`: Function call context for maintaining state between calls
- `fctx`: User-defined context structure containing current, finish, and step values

## Dependencies
- Functions called/Symbols referenced:
  - `SRF_IS_FIRSTCALL` - Macro to check if this is the first function call
  - `PG_GETARG_INT32` - Macro to extract int32 arguments
  - `PG_NARGS` - Macro to get the number of arguments
  - `SRF_FIRSTCALL_INIT` - [Initialize](../I/Initialize.md) SRF context on first call
  - `SRF_PERCALL_SETUP` - Setup for each subsequent call
  - `[pg_add_s32_overflow](../p/pg_add_s32_overflow.md)` - Safe integer addition with overflow detection
  - `SRF_RETURN_NEXT` - Return next value in the series
  - `SRF_RETURN_DONE` - Signal end of series
  - `generate_series_fctx` - Context structure for maintaining state
  - `[FuncCallContext](../F/FuncCallContext.md)` - PostgreSQL function call context structure
- Called from (representative examples):
  - `[generate_series_int4](generate_series_int4.md)` - Wrapper function for two-parameter version

## Notes and Other Information
- Located in `src/backend/utils/adt/int.c:1509-1584`
- Validates that step size is not zero, throwing an error if it is
- Uses memory context switching for proper memory management across multiple calls
- Handles both ascending (positive step) and descending (negative step) series
- Includes overflow protection using `pg_add_s32_overflow` to prevent infinite loops
- Part of PostgreSQL's generate_series() SQL function family
- Can handle 2 or 3 parameters (start, finish, and optional step)
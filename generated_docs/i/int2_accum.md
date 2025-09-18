# int2_accum

## Location
src/backend/utils/adt/numeric.c: 5566 - 5588

## Overview
PostgreSQL aggregate function that accumulates smallint (int2) values for statistical calculations like variance, standard deviation, and population statistics.

## Definition
```c
Datum int2_accum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the transition function for various statistical aggregates operating on smallint (int2) values. It maintains running totals including sum, count, and sum of squares in a `PolyNumAggState` structure. The function handles both the initialization of the state on the first call and the accumulation of subsequent values. It supports both 128-bit integer arithmetic (when available) and falls back to numeric arithmetic for broader compatibility.

## Parameters / Member Variables
- Uses PostgreSQL's `PG_FUNCTION_ARGS` macro to access function arguments:
  - Argument 0: Existing aggregate state (internal type, can be NULL for first call)
  - Argument 1: New smallint value to accumulate (int2, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `makePolyNumAggState`: Creates new aggregate state when needed
  - [do_int128_accum](../d/do_int128_accum.md): Performs 128-bit integer accumulation (when `HAVE_INT128` is defined)
  - [do_numeric_accum](../d/do_numeric_accum.md): Performs numeric accumulation (fallback)
  - [int64_to_numeric](int64_to_numeric.md): Converts integer to numeric type
  - `PG_GETARG_INT16`: Extracts smallint argument
- Called from (representative examples):
  - Used as transition function in aggregate definitions for `var_pop(int2)`
  - Used as transition function in aggregate definitions for `var_samp(int2)`
  - Used as transition function in aggregate definitions for `variance(int2)`
  - Used as transition function in aggregate definitions for `stddev_pop(int2)`
  - Used as transition function in aggregate definitions for `stddev_samp(int2)`

## Notes and Other Information
- This is a PostgreSQL built-in function exposed in pg_proc.dat
- The function is not marked as strict (`proisstrict => 'f'`), allowing it to handle NULL inputs
- Uses conditional compilation to choose between 128-bit integer and numeric arithmetic
- The state is created with `calcSumX2 = true` to support variance and standard deviation calculations
- Part of PostgreSQL's polymorphic numeric aggregate system
- Returns internal type pointer to maintain state across aggregate calls
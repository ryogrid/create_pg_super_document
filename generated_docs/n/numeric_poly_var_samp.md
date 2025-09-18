# numeric_poly_var_samp

## Location
src/backend/utils/adt/numeric.c: 6420 - 6440

## Overview
Computes the sample variance using polymorphic aggregation with 128-bit integer optimization, providing the final result for the VAR_SAMP() aggregate function.

## Definition


## Detailed Description
The `numeric_poly_var_samp` function is a PostgreSQL aggregate function finalizer that calculates sample variance from accumulated numeric values using an optimized polymorphic approach. When 128-bit integer support is available (HAVE_INT128), it uses `PolyNumAggState` which stores accumulated values as 128-bit integers for better performance, then converts them via `numeric_poly_stddev_internal`. When 128-bit support is not available, it falls back to the standard `numeric_var_samp` function. The sample variance uses N-1 in the denominator rather than N (population variance).

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `state`: PolyNumAggState pointer containing accumulated statistics in 128-bit integer format

## Dependencies
- Functions called/Symbols referenced:
  - `numeric_poly_stddev_internal` (performs variance calculation with 128-bit integer state)
  - `numeric_var_samp` (fallback for systems without 128-bit integer support)
  - `PolyNumAggState` (polymorphic aggregate state with 128-bit integers)
  - `Numeric` (PostgreSQL numeric data type)
  - `PG_RETURN_NUMERIC` (macro for returning numeric values)
- Called from (representative examples):
  - Used as finalizer for VAR_SAMP() aggregate function

## Dependencies
- Functions called/Symbols referenced:
  - `numeric_poly_stddev_internal` (performs variance calculation with 128-bit integer state)
  - `numeric_var_samp` (fallback for systems without 128-bit integer support)
  - `PolyNumAggState` (polymorphic aggregate state with 128-bit integers)
  - `Numeric` (PostgreSQL numeric data type)
  - `PG_RETURN_NUMERIC` (macro for returning numeric values)
- Called from (representative examples):
  - Used as finalizer for VAR_SAMP() aggregate function

## Notes and Other Information
- This is a PostgreSQL aggregate function finalizer, called at the end of aggregation
- Uses conditional compilation (`#ifdef HAVE_INT128`) to optimize for platforms with 128-bit integer support
- Returns NULL if the input state is NULL or if no valid data was accumulated
- Uses sample variance formula (divides by N-1) rather than population variance (divides by N)
- Part of PostgreSQL's polymorphic aggregate optimization that can provide significant performance improvements
- Falls back gracefully to standard numeric operations on platforms without 128-bit integer support
- The polymorphic approach allows the same aggregate function to work efficiently with different numeric types
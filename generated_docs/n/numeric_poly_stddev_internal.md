# numeric_poly_stddev_internal

## Location
src/backend/utils/adt/numeric.c: 6375 - 6419

## Overview
A static internal function that converts Int128-based aggregate state to NumericAggState and delegates to numeric_stddev_internal for variance and standard deviation calculations.

## Definition


## Detailed Description
The `numeric_poly_stddev_internal` function serves as an adapter between the 128-bit integer-based aggregate state (`Int128AggState`) used by polymorphic aggregate functions and the numeric-based aggregate state (`NumericAggState`) required by the core variance/standard deviation calculation function. It converts the accumulated 128-bit integer sums (sumX and sumX2) to numeric format, then delegates the actual statistical computation to `numeric_stddev_internal`. This function is part of PostgreSQL's polymorphic aggregate system that can handle different numeric types efficiently using 128-bit integer arithmetic during accumulation.

## Parameters / Member Variables
- `state`: Int128AggState pointer containing accumulated statistics using 128-bit integers
- `variance`: Boolean flag indicating whether to compute variance (true) or standard deviation (false)
- `sample`: Boolean flag indicating whether to use sample (true) or population (false) formula
- `is_null`: Output parameter indicating if the result should be NULL

## Dependencies
- Functions called/Symbols referenced:
  - [numeric_stddev_internal](numeric_stddev_internal.md) (core variance/standard deviation calculation)
  - [int128_to_numericvar](../i/int128_to_numericvar.md) (converts 128-bit integer to NumericVar)
  - [accum_sum_add](../a/accum_sum_add.md) (adds numeric values to accumulator)
  - `init_var` (initializes NumericVar)
  - [free_var](../f/free_var.md) (frees NumericVar memory)
  - [Int128AggState](../I/Int128AggState.md) (128-bit integer aggregate state)
  - [NumericAggState](../N/NumericAggState.md) (numeric aggregate state)
- Called from (representative examples):
  - [numeric_poly_var_samp](numeric_poly_var_samp.md) (sample variance)
  - [numeric_poly_stddev_samp](numeric_poly_stddev_samp.md) (sample standard deviation)
  - [numeric_poly_var_pop](numeric_poly_var_pop.md) (population variance)
  - [numeric_poly_stddev_pop](numeric_poly_stddev_pop.md) (population standard deviation)

## Notes and Other Information
- This is a static (internal) function not exposed outside the numeric.c file
- Performs careful memory management, freeing allocated digits arrays after computation
- Handles NULL state by initializing an empty NumericAggState
- Part of PostgreSQL's optimization strategy using 128-bit integer arithmetic for better performance during aggregate accumulation
- The conversion from Int128 to Numeric format allows reuse of existing statistical calculation logic
- Maintains proper memory cleanup to prevent leaks in long-running aggregations
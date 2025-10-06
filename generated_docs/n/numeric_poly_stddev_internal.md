# numeric_poly_stddev_internal

## Location
[src/backend/utils/adt/numeric.c:6375-6419](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6375-L6419)

## Overview
A static internal function that converts Int128-based aggregate state to NumericAggState and delegates to numeric_stddev_internal for variance and standard deviation calculations.

## Definition

```c
static Numeric
numeric_poly_stddev_internal(Int128AggState *state,
							 bool variance, bool sample,
							 bool *is_null)
```
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

## Simplified Source

```c
static Numeric numeric_poly_stddev_internal(Int128AggState *state, bool variance, bool sample, bool *is_null) {
    NumericAggState numeric_state;
    Numeric result;

    // Initialize empty numeric aggregate state
    memset(&numeric_state, 0, sizeof(NumericAggState));

    if (state) {
        NumericVar temp_var;

        // Copy count from 128-bit state
        numeric_state.N = state->N;

        init_var(&temp_var);

        // Convert 128-bit sumX to numeric and add to accumulator
        int128_to_numericvar(state->sumX, &temp_var);
        accum_sum_add(&numeric_state.sumX, &temp_var);

        // Convert 128-bit sumX2 to numeric and add to accumulator
        int128_to_numericvar(state->sumX2, &temp_var);
        accum_sum_add(&numeric_state.sumX2, &temp_var);

        free_var(&temp_var);
    }

    // Delegate to standard numeric variance/stddev calculation
    result = numeric_stddev_internal(&numeric_state, variance, sample, is_null);

    // Clean up allocated memory from numeric state
    if (numeric_state.sumX.ndigits > 0) {
        pfree(numeric_state.sumX.pos_digits);
        pfree(numeric_state.sumX.neg_digits);
    }
    if (numeric_state.sumX2.ndigits > 0) {
        pfree(numeric_state.sumX2.pos_digits);
        pfree(numeric_state.sumX2.neg_digits);
    }

    return result;
}
```
# do_numeric_accum

## Location
[src/backend/utils/adt/numeric.c:4873-4942](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4873-L4942)

## Overview
Accumulates a new numeric input value into a NumericAggState structure, handling special values and maintaining running sums for aggregate functions.

## Definition

```c
static void
do_numeric_accum(NumericAggState *state, Numeric newval)
```
## Detailed Description
This static function performs the core accumulation logic for numeric aggregate functions. It handles special numeric values (positive infinity, negative infinity, NaN) by maintaining separate counters for each type. For regular numeric values, it tracks the maximum decimal scale encountered (needed for inverse transitions), converts the input to variable format, optionally computes the square of the value if required, and then accumulates the value and its square (if needed) into running sums. The function carefully manages memory contexts, performing calculations in short-lived contexts but storing accumulated results in the aggregate context to ensure proper memory management throughout the aggregate operation.

## Parameters / Member Variables
- `*state`: Pointer to NumericAggState structure containing aggregate state (counters, sums, context information)
- `newval`: The new numeric value to accumulate into the aggregate state
## Dependencies
- Functions called/Symbols referenced:
  -  - Check if numeric value is special (NaN, infinity)
  -  - Check for positive infinity
  -  - Check for negative infinity
  -  - Convert numeric to NumericVar format
  -  - [Initialize](../I/Initialize.md) NumericVar structure
  -  - Multiply two NumericVar values
  -  - Add value to accumulated sum
  -  - Switch memory contexts
- Called from (representative examples):
  -  - Standard numeric accumulation function
  -  - [Numeric](../N/Numeric.md) average accumulation
  -  - 16-bit integer accumulation
  -  - 32-bit integer accumulation
  -  - 64-bit integer accumulation
  -  - 64-bit integer average accumulation

## Notes and Other Information
- Declared as static, limiting visibility to numeric.c file
- Maintains separate counters for NaN, positive infinity, and negative infinity
- Tracks maximum decimal scale for supporting inverse aggregate transitions
- Uses memory context switching to ensure proper allocation of persistent vs temporary data
- Computes sum of squares only when requested (controlled by state->calcSumX2 flag)
- Essential building block for PostgreSQL's numeric aggregate functions (SUM, AVG, etc.)
- Carefully handles precision by using appropriate decimal scales for squared values
- Located in src/backend/utils/adt/numeric.c:4873-4942

## Simplified Source

```c
static void do_numeric_accum(NumericAggState *state, Numeric newval) {
    NumericVar X, X2;
    MemoryContext old_context;

    // Handle special values (NaN, infinity) separately - just count them
    if (NUMERIC_IS_SPECIAL(newval)) {
        if (NUMERIC_IS_PINF(newval))
            state->pInfcount++;
        else if (NUMERIC_IS_NINF(newval))
            state->nInfcount++;
        else
            state->NaNcount++;
        return;
    }

    // Convert numeric to variable format for processing
    init_var_from_num(newval, &X);

    // Track maximum decimal scale for inverse transitions
    if (X.dscale > state->maxScale) {
        state->maxScale = X.dscale;
        state->maxScaleCount = 1;
    } else if (X.dscale == state->maxScale) {
        state->maxScaleCount++;
    }

    // Calculate X^2 if needed for variance calculations
    if (state->calcSumX2) {
        init_var(&X2);
        mul_var(&X, &X, &X2, X.dscale * 2);
    }

    // Switch to aggregate context for persistent data
    old_context = MemoryContextSwitchTo(state->agg_context);

    // Increment count and accumulate sums
    state->N++;
    accum_sum_add(&(state->sumX), &X);
    if (state->calcSumX2)
        accum_sum_add(&(state->sumX2), &X2);

    MemoryContextSwitchTo(old_context);
}
```
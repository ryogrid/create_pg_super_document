# numeric_poly_combine

## Location
[src/backend/utils/adt/numeric.c:5632-5696](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5632-L5696)

## Overview
The numeric_poly_combine function is a combine function for PostgreSQL's parallel aggregation framework, merging two PolyNumAggState structures for numeric aggregates that require both sum and sum-of-squares calculations.

## Definition
```c
Datum numeric_poly_combine(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's parallel aggregation infrastructure, specifically designed to combine partial aggregate results from different worker processes or segments. It merges two PolyNumAggState structures, combining their statistical accumulation data including count (N), sum of values (sumX), and sum of squares (sumX2). This is essential for statistical aggregates like variance, standard deviation, and covariance that require squared terms.

The function handles various edge cases: if either state is NULL, it returns the other state appropriately. When both states exist, it combines them by adding counts and sums. The function uses conditional compilation to optimize performance on platforms supporting 128-bit arithmetic, falling back to numeric arithmetic operations when necessary.

Memory management is carefully handled to ensure all operations occur in the proper aggregate memory context.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - Argument 0: PolyNumAggState pointer (first state to combine, can be NULL)
  - Argument 1: PolyNumAggState pointer (second state to combine, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PolyNumAggState (data structure)
  - [AggCheckCallContext](../A/AggCheckCallContext.md) (context validation)
  - makePolyNumAggState (state initialization)
  - [accum_sum_copy](../a/accum_sum_copy.md) (sum copying for non-int128 path)
  - [accum_sum_combine](../a/accum_sum_combine.md) (sum combining for non-int128 path)
  - [MemoryContext](../M/MemoryContext.md) management functions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a combine function specifically for parallel aggregation in PostgreSQL's aggregate framework
- Uses conditional compilation with HAVE_INT128 for performance optimization on supported platforms
- Properly manages PostgreSQL memory contexts to ensure correct memory allocation
- Essential for statistical aggregates that need both sum and sum-of-squares values
- Part of the polymorphic numeric aggregate system that can efficiently handle different numeric input types
- The function validates that it's called in an appropriate aggregate context and will error if called incorrectly

## Simplified Source

```c
Datum
numeric_poly_combine(PG_FUNCTION_ARGS)
{
    PolyNumAggState *state1;
    PolyNumAggState *state2;
    MemoryContext agg_context;
    MemoryContext old_context;

    // Validate aggregate context
    if (!AggCheckCallContext(fcinfo, &agg_context))
        elog(ERROR, "aggregate function called in non-aggregate context");

    // Get the two input states
    state1 = PG_ARGISNULL(0) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(0);
    state2 = PG_ARGISNULL(1) ? NULL : (PolyNumAggState *) PG_GETARG_POINTER(1);

    // If state2 is NULL, just return state1
    if (state2 == NULL)
        PG_RETURN_POINTER(state1);

    // If state1 is NULL, create new state and copy all data from state2
    if (state1 == NULL)
    {
        old_context = MemoryContextSwitchTo(agg_context);

        state1 = makePolyNumAggState(fcinfo, true);
        state1->N = state2->N;

#ifdef HAVE_INT128
        state1->sumX = state2->sumX;
        state1->sumX2 = state2->sumX2;
#else
        accum_sum_copy(&state1->sumX, &state2->sumX);
        accum_sum_copy(&state1->sumX2, &state2->sumX2);
#endif

        MemoryContextSwitchTo(old_context);
        PG_RETURN_POINTER(state1);
    }

    // Combine the two states
    if (state2->N > 0)
    {
        state1->N += state2->N;

#ifdef HAVE_INT128
        state1->sumX += state2->sumX;
        state1->sumX2 += state2->sumX2;
#else
        // Use numeric arithmetic for platforms without 128-bit support
        old_context = MemoryContextSwitchTo(agg_context);
        accum_sum_combine(&state1->sumX, &state2->sumX);
        accum_sum_combine(&state1->sumX2, &state2->sumX2);
        MemoryContextSwitchTo(old_context);
#endif
    }

    PG_RETURN_POINTER(state1);
}
```
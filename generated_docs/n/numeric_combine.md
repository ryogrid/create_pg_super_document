# numeric_combine

## Location
[src/backend/utils/adt/numeric.c:5056-5127](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5056-L5127)

## Overview
A PostgreSQL combine function for numeric aggregates that require both sum (sumX) and sum of squares (sumX2) calculations, used to merge partial aggregate states in parallel query execution and window functions.

## Definition

```c
Datum
numeric_combine(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the combine operation for numeric aggregates that maintain both sumX and sumX2 values (like variance and standard deviation). It's essential for PostgreSQL's parallel aggregation capabilities, where partial aggregates computed by different worker processes need to be combined into a final result.

The function handles several scenarios:
- **State Creation**: If state1 is NULL, it creates a new state and copies all data from state2
- **State Merging**: When both states exist, it combines their counts, special value counts (NaN, infinity), scale information, and accumulated sums
- **Scale Management**: Properly maintains the maximum scale (dscale) information needed for accurate numeric operations
- **Memory Management**: Uses appropriate memory contexts to ensure data persists across aggregate operations

The combining process involves adding counts, merging scale tracking data, and using accum_sum_combine to properly merge the accumulated sums and sums of squares.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention where:
  - Argument 0: First aggregate state (NumericAggState pointer, may be NULL)
  - Argument 1: Second aggregate state (NumericAggState pointer, may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - [AggCheckCallContext](../A/AggCheckCallContext.md)
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - [makeNumericAggStateCurrentContext](../m/makeNumericAggStateCurrentContext.md)
  - [accum_sum_copy](../a/accum_sum_copy.md)
  - [accum_sum_combine](../a/accum_sum_combine.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - PG_RETURN_POINTER
  - elog
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's aggregate function catalog)

## Notes and Other Information
- Critical for PostgreSQL's parallel query execution where aggregates are computed in parallel and then combined
- Handles all aspects of state merging: counts (N), special value counts (NaN, positive/negative infinity), scale tracking, and accumulated values
- The maxScale and maxScaleCount management is particularly important for maintaining numeric precision
- Uses accum_sum_combine for mathematically correct combination of accumulated sums and sums of squares
- Proper memory context management ensures combined state data persists appropriately
- The function validates it's called in an appropriate aggregate context using AggCheckCallContext
- Designed to work with makeNumericAggStateCurrentContext(true) which indicates sumX2 calculation is required

## Simplified Source

```c
// Simplified version of numeric_combine
Datum numeric_combine(PG_FUNCTION_ARGS) {
    NumericAggState *state1;
    NumericAggState *state2;
    MemoryContext agg_context;
    MemoryContext old_context;

    // Step 1: Validate this is called in proper aggregate context
    if (!AggCheckCallContext(fcinfo, &agg_context)) {
        elog(ERROR, "aggregate function called in non-aggregate context");
    }

    // Step 2: Extract the two aggregate states to combine
    state1 = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);
    state2 = PG_ARGISNULL(1) ? NULL : (NumericAggState *) PG_GETARG_POINTER(1);

    // Step 3: Handle NULL states
    if (state2 == NULL) {
        // Nothing to combine - return first state as-is
        PG_RETURN_POINTER(state1);
    }

    if (state1 == NULL) {
        // Create new state from state2
        old_context = MemoryContextSwitchTo(agg_context);

        state1 = makeNumericAggStateCurrentContext(true);  // true = needs sumX2

        // Copy all fields from state2 to new state1
        state1->N = state2->N;
        state1->NaNcount = state2->NaNcount;
        state1->pInfcount = state2->pInfcount;
        state1->nInfcount = state2->nInfcount;
        state1->maxScale = state2->maxScale;
        state1->maxScaleCount = state2->maxScaleCount;

        // Copy accumulated sums
        accum_sum_copy(&state1->sumX, &state2->sumX);
        accum_sum_copy(&state1->sumX2, &state2->sumX2);

        MemoryContextSwitchTo(old_context);
        PG_RETURN_POINTER(state1);
    }

    // Step 4: Merge both states
    // Combine basic counts
    state1->N += state2->N;
    state1->NaNcount += state2->NaNcount;
    state1->pInfcount += state2->pInfcount;
    state1->nInfcount += state2->nInfcount;

    // Step 5: Handle scale information (for precision tracking)
    if (state2->N > 0) {
        // Merge scale tracking information
        if (state2->maxScale > state1->maxScale) {
            // state2 has higher precision
            state1->maxScale = state2->maxScale;
            state1->maxScaleCount = state2->maxScaleCount;
        }
        else if (state2->maxScale == state1->maxScale) {
            // Same precision - add counts
            state1->maxScaleCount += state2->maxScaleCount;
        }

        // Step 6: Combine accumulated sums in proper memory context
        old_context = MemoryContextSwitchTo(agg_context);

        // Mathematically combine the sums and sums of squares
        accum_sum_combine(&state1->sumX, &state2->sumX);    // Σx
        accum_sum_combine(&state1->sumX2, &state2->sumX2);  // Σx²

        MemoryContextSwitchTo(old_context);
    }

    PG_RETURN_POINTER(state1);
}
```

Key simplifications made:
- Added clear step-by-step comments explaining each phase of the combine operation
- Organized the logic into logical sections: validation, state handling, merging
- Simplified variable organization and eliminated intermediate variables
- Made the three main cases explicit: NULL state2, NULL state1, both states exist
- Clarified the purpose of scale tracking (precision management)
- Added comments explaining the mathematical operations (Σx and Σx²)
- Made memory context switching more explicit and understandable
- Focused on the core aggregate combining algorithm rather than low-level details
- Preserved all essential numeric aggregation functionality
# numeric_combine

## Location
src/backend/utils/adt/numeric.c: 5056 - 5127

## Overview
A PostgreSQL combine function for numeric aggregates that require both sum (sumX) and sum of squares (sumX2) calculations, used to merge partial aggregate states in parallel query execution and window functions.

## Definition


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
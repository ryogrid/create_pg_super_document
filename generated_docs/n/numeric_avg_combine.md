# numeric_avg_combine

## Location
src/backend/utils/adt/numeric.c: 5148 - 5219

## Overview
A PostgreSQL combine function for numeric aggregates that only require sum (sumX) calculations, used to merge partial aggregate states for simpler operations like AVG and SUM in parallel query execution.

## Definition


## Detailed Description
This function implements the combine operation for numeric aggregates that only maintain sumX values (not sumX2), such as average (AVG) and sum (SUM) operations. It's essential for PostgreSQL's parallel aggregation capabilities, where partial aggregates computed by different worker processes need to be combined into a final result.

The key difference from numeric_combine is that this function only handles sumX (not sumX2), making it more efficient for simpler aggregates. The function handles several scenarios:
- **State Creation**: If state1 is NULL, it creates a new state with calcSumX2=false and copies all data from state2
- **State Merging**: When both states exist, it combines their counts, special value counts (NaN, infinity), scale information, and accumulated sum (but not sum of squares)
- **Scale Management**: Properly maintains the maximum scale (dscale) information needed for accurate numeric operations  
- **Memory Management**: Uses appropriate memory contexts to ensure data persists across aggregate operations

The combining process involves adding counts, merging scale tracking data, and using accum_sum_combine to properly merge only the accumulated sum (not sum of squares).

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention where:
  - Argument 0: First aggregate state (NumericAggState pointer, may be NULL)
  - Argument 1: Second aggregate state (NumericAggState pointer, may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - AggCheckCallContext
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - makeNumericAggStateCurrentContext
  - accum_sum_copy
  - accum_sum_combine
  - MemoryContextSwitchTo
  - PG_RETURN_POINTER
  - elog
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's aggregate function catalog)

## Notes and Other Information
- Critical for PostgreSQL's parallel query execution for simpler aggregates (AVG, SUM) that don't need sum of squares
- More efficient than numeric_combine since it only handles sumX, not sumX2
- Handles all aspects of state merging: counts (N), special value counts (NaN, positive/negative infinity), scale tracking, and accumulated sum
- The maxScale and maxScaleCount management is important for maintaining numeric precision
- Uses accum_sum_combine for mathematically correct combination of accumulated sums only
- Uses makeNumericAggStateCurrentContext(false) indicating sumX2 calculation is NOT required
- Proper memory context management ensures combined state data persists appropriately
- The function validates it's called in an appropriate aggregate context using AggCheckCallContext
- Complements numeric_combine by providing an optimized path for simpler aggregates
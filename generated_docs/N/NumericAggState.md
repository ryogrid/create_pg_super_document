# NumericAggState

## Location
src/backend/utils/adt/numeric.c: 4810 - 4823

## Overview
NumericAggState is a structure used to maintain state information during numeric aggregate operations in PostgreSQL, storing intermediate calculation results for functions like SUM, AVG, and statistical aggregates.

## Definition


## Detailed Description
NumericAggState serves as the transition datatype for PostgreSQL's numeric aggregate functions. It maintains comprehensive state information needed to compute various statistical and mathematical aggregates over numeric values. The structure handles both basic aggregates (sum, count) and more complex statistical functions (variance, standard deviation) by optionally calculating sum of squares. It also properly handles special numeric values like NaN and infinity, maintaining separate counts for these cases that don't interfere with normal calculations.

## Parameters / Member Variables
- : Boolean flag indicating whether to calculate sum of squares (needed for variance/stddev)
- : Memory context where the aggregate calculation is performed
- : Count of processed numeric values (excludes NaN and infinity values)
- : Accumulated sum of all processed numeric values
- : Accumulated sum of squares of processed values (when calcSumX2 is true)
- : Maximum decimal scale encountered among processed values
- : Number of values that had the maximum scale
- : Count of NaN (Not a Number) values encountered
- : Count of positive infinity values encountered
- : Count of negative infinity values encountered

## Dependencies
- Functions called/Symbols referenced:
  - NumericSumAccum (for sumX and sumX2 members)
- Called from (representative examples):
  - makeNumericAggState
  - makeNumericAggStateCurrentContext
  - do_numeric_accum
  - do_numeric_discard
  - numeric_accum
  - numeric_combine
  - numeric_avg_accum
  - numeric_avg_combine
  - numeric_serialize
  - numeric_deserialize
  - numeric_accum_inv
  - int8_accum
  - int8_accum_inv
  - numeric_avg
  - numeric_sum
  - numeric_stddev_internal
  - numeric_var_samp
  - numeric_stddev_samp
  - numeric_var_pop
  - numeric_stddev_pop
  - numeric_poly_stddev_internal

## Notes and Other Information
This structure is used as the INTERNAL transition datatype for PostgreSQL's numeric aggregate functions. The actual implementation stores this as a pointer allocated in the aggregate context, with digit buffers for NumericVars also allocated in the same context. On platforms supporting 128-bit integers, some aggregates may use Int128AggState instead for performance optimization. The special value counts (NaN, +Inf, -Inf) are maintained separately from the main count N and should be accessed using the NA_TOTAL_COUNT() macro when the total count is needed.
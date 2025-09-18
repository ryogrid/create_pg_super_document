# NumericAggState

## Location
[src/backend/utils/adt/numeric.c:4810-4823](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L4810-L4823)

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
  - [NumericSumAccum](NumericSumAccum.md) (for sumX and sumX2 members)
- Called from (representative examples):
  - [makeNumericAggState](../m/makeNumericAggState.md)
  - [makeNumericAggStateCurrentContext](../m/makeNumericAggStateCurrentContext.md)
  - [do_numeric_accum](../d/do_numeric_accum.md)
  - [do_numeric_discard](../d/do_numeric_discard.md)
  - [numeric_accum](../n/numeric_accum.md)
  - [numeric_combine](../n/numeric_combine.md)
  - [numeric_avg_accum](../n/numeric_avg_accum.md)
  - [numeric_avg_combine](../n/numeric_avg_combine.md)
  - [numeric_serialize](../n/numeric_serialize.md)
  - [numeric_deserialize](../n/numeric_deserialize.md)
  - [numeric_accum_inv](../n/numeric_accum_inv.md)
  - [int8_accum](../i/int8_accum.md)
  - [int8_accum_inv](../i/int8_accum_inv.md)
  - [numeric_avg](../n/numeric_avg.md)
  - [numeric_sum](../n/numeric_sum.md)
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md)
  - [numeric_var_samp](../n/numeric_var_samp.md)
  - [numeric_stddev_samp](../n/numeric_stddev_samp.md)
  - [numeric_var_pop](../n/numeric_var_pop.md)
  - [numeric_stddev_pop](../n/numeric_stddev_pop.md)
  - [numeric_poly_stddev_internal](../n/numeric_poly_stddev_internal.md)

## Notes and Other Information
This structure is used as the INTERNAL transition datatype for PostgreSQL's numeric aggregate functions. The actual implementation stores this as a pointer allocated in the aggregate context, with digit buffers for NumericVars also allocated in the same context. On platforms supporting 128-bit integers, some aggregates may use Int128AggState instead for performance optimization. The special value counts (NaN, +Inf, -Inf) are maintained separately from the main count N and should be accessed using the NA_TOTAL_COUNT() macro when the total count is needed.
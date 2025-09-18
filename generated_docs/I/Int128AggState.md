# Int128AggState

## Location
src/backend/utils/adt/numeric.c: 5483 - 5489

## Overview
Int128AggState is a high-performance structure used for numeric aggregate operations on platforms supporting 128-bit integers, providing faster calculations compared to NumericAggState for compatible aggregate functions.

## Definition


## Detailed Description
Int128AggState is an optimized version of NumericAggState that leverages 128-bit integer arithmetic for improved performance in numeric aggregate calculations. This structure is used on platforms that support 128-bit integers and provides a more efficient transition datatype for aggregates that can fit within the 128-bit integer range. Unlike NumericAggState, it doesn't track special values (NaN, infinity) or memory context information, focusing purely on fast integer-based calculations.

## Parameters / Member Variables
- : Boolean flag indicating whether to calculate sum of squares (required for variance and standard deviation)
- : Count of processed numeric values
- : Accumulated sum of all processed values using 128-bit integer arithmetic
- : Accumulated sum of squares of processed values (when calcSumX2 is true)

## Dependencies
- Functions called/Symbols referenced:
  - int128 (primitive 128-bit integer type)
- Called from (representative examples):
  - [makeInt128AggState](../m/makeInt128AggState.md)
  - [makeInt128AggStateCurrentContext](../m/makeInt128AggStateCurrentContext.md)
  - [do_int128_accum](../d/do_int128_accum.md)
  - [do_int128_discard](../d/do_int128_discard.md)
  - [numeric_poly_stddev_internal](../n/numeric_poly_stddev_internal.md)

## Notes and Other Information
This structure is used as an alternative to NumericAggState on platforms that support 128-bit integers for performance optimization. It provides faster arithmetic operations but has limitations compared to NumericAggState: it cannot handle arbitrary precision numeric values, special values like NaN or infinity, or values that exceed the 128-bit integer range. The choice between Int128AggState and NumericAggState is made at runtime based on the platform capabilities and the nature of the values being aggregated. This optimization is particularly beneficial for integer-based aggregates and numeric values that can be represented within the 128-bit range.
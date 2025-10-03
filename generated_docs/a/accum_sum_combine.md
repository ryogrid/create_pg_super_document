# accum_sum_combine

## Location
[src/backend/utils/adt/numeric.c:12270-12280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L12270-L12280)

## Overview
A static utility function that combines one NumericSumAccum accumulator into another, used for merging partial sum results in PostgreSQL's numeric aggregation operations.

## Definition

```c
static void
accum_sum_combine(NumericSumAccum *accum, NumericSumAccum *accum2)
```
## Detailed Description
The  function is designed to merge two NumericSumAccum accumulators by adding the final value of the second accumulator (accum2) into the first accumulator (accum). This operation is essential for parallel aggregation and combining partial results in PostgreSQL's numeric sum operations.

The function works by:
1. Creating a temporary NumericVar to hold the finalized value from accum2
2. Calling accum_sum_final() to compute the final numeric result from accum2
3. Adding this final result to accum using accum_sum_add()
4. Cleaning up the temporary variable

This approach leverages the existing accumulator infrastructure to efficiently combine results without directly manipulating the internal digit arrays.

## Parameters / Member Variables
- `*accum`: Target NumericSumAccum accumulator that will receive the combined result
- `*accum2`: Source NumericSumAccum accumulator whose value will be added to accum
## Dependencies
- Functions called/Symbols referenced:
  - init_var (initializes temporary NumericVar)
  - [accum_sum_final](accum_sum_final.md) (computes final result from accum2)
  - [accum_sum_add](accum_sum_add.md) (adds the result to target accumulator)
  - [free_var](../f/free_var.md) (cleans up temporary variable)
  - [NumericSumAccum](../N/NumericSumAccum.md) (struct type for fast sum accumulation)
- Called from (representative examples):
  - [numeric_combine](../n/numeric_combine.md)
  - [numeric_avg_combine](../n/numeric_avg_combine.md)  
  - [numeric_poly_combine](../n/numeric_poly_combine.md)
  - [int8_avg_combine](../i/int8_avg_combine.md)

## Notes and Other Information
- This is a static function, only accessible within src/backend/utils/adt/numeric.c
- Part of PostgreSQL's optimized numeric aggregation system that uses 32-bit integers for faster accumulation
- Used primarily in parallel aggregation scenarios where partial results from different workers need to be combined
- The function preserves the precision and scale requirements of numeric operations
- Does not handle NaN values - this is managed at higher levels of the aggregation system
- Location: src/backend/utils/adt/numeric.c:12270-12280
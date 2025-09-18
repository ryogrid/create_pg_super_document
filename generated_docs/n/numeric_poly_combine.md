# numeric_poly_combine

## Location
src/backend/utils/adt/numeric.c: 5632 - 5696

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
  - AggCheckCallContext (context validation)
  - makePolyNumAggState (state initialization)
  - accum_sum_copy (sum copying for non-int128 path)
  - accum_sum_combine (sum combining for non-int128 path)
  - MemoryContext management functions
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a combine function specifically for parallel aggregation in PostgreSQL's aggregate framework
- Uses conditional compilation with HAVE_INT128 for performance optimization on supported platforms
- Properly manages PostgreSQL memory contexts to ensure correct memory allocation
- Essential for statistical aggregates that need both sum and sum-of-squares values
- Part of the polymorphic numeric aggregate system that can efficiently handle different numeric input types
- The function validates that it's called in an appropriate aggregate context and will error if called incorrectly
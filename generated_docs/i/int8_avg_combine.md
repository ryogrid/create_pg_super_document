# int8_avg_combine

## Location
src/backend/utils/adt/numeric.c: 5835 - 5894

## Overview
Combine function for PolyNumAggState that merges two aggregation states for aggregates that don't require sumX2 (sum of squares).

## Definition
```c
Datum int8_avg_combine(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the combine operation for parallel aggregation in PostgreSQL. It merges two PolyNumAggState structures, combining their counts (N) and sums (sumX) to produce a unified state. The function is specifically designed for aggregates that don't need sum of squares calculations, making it suitable for simple average operations.

The function handles various edge cases including NULL states and ensures proper memory context management for aggregate operations. It supports both 128-bit integer arithmetic (when available) and falls back to numeric arithmetic operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: PostgreSQL function calling convention macro that provides access to:
  - Arg 0: PolyNumAggState pointer (first state, can be NULL)
  - Arg 1: PolyNumAggState pointer (second state, can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - `PolyNumAggState` (structure type)
  - `[AggCheckCallContext](../A/AggCheckCallContext.md)` (validates aggregate context)
  - `makePolyNumAggState` (creates new state)
  - `[accum_sum_copy](../a/accum_sum_copy.md)` (copies sum for numeric version)
  - `[accum_sum_combine](../a/accum_sum_combine.md)` (combines sums for numeric version)
  - `[MemoryContext](../M/MemoryContext.md)` operations for proper memory management
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's parallel aggregate system)

## Notes and Other Information
- Essential for PostgreSQL's parallel query execution when combining partial aggregate results
- Uses conditional compilation with HAVE_INT128 for optimized 128-bit arithmetic
- Implements proper memory context switching to ensure aggregate state persists correctly
- Returns the first state with combined values, or creates new state if first is NULL
- Part of PostgreSQL's polymorphic numeric aggregation framework
# int4_avg_combine

## Location
src/backend/utils/adt/numeric.c: 6729 - 6759

## Overview
PostgreSQL aggregate combine function that merges two int4 average transition states during parallel aggregation, combining their counts and sums.

## Definition
```c
Datum int4_avg_combine(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the combine function for AVG() aggregates over int4 (integer) data types in parallel aggregation scenarios. When PostgreSQL executes aggregates in parallel, multiple workers may accumulate partial results independently. This function combines two such partial states by adding their counts and sums together.

The function requires that both input states are valid Int8TransTypeData arrays and strictly enforces aggregate context calling. It combines the second state into the first state and returns the merged result.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Arg 0: ArrayType* - The first transition state array (target for merge)
  - Arg 1: ArrayType* - The second transition state array (source for merge)

## Dependencies
- Functions called/Symbols referenced:
  - Int8TransTypeData
  - AggCheckCallContext
  - PG_GETARG_ARRAYTYPE_P
  - ARR_HASNULL
  - ARR_SIZE
  - ARR_OVERHEAD_NONULLS
  - ARR_DATA_PTR
  - PG_RETURN_ARRAYTYPE_P
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL parallel aggregate system)

## Notes and Other Information
- Essential for parallel execution of AVG() aggregates over integer columns
- Strictly validates that it's called in aggregate context, unlike accumulation functions
- Validates both input transition arrays to ensure they contain expected Int8TransTypeData structures  
- Modifies the first transition state in-place and returns it as the combined result
- Part of PostgreSQL's parallel aggregation framework introduced for performance optimization
- Works by simply adding counts and sums from both partial states
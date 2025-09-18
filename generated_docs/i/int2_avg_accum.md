# int2_avg_accum

## Location
src/backend/utils/adt/numeric.c: 6673 - 6700

## Overview
PostgreSQL aggregate transition function that accumulates int2 (smallint) values for computing the average, maintaining both a sum and count in an internal transition state.

## Definition
```c
Datum int2_avg_accum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the transition function for AVG() aggregates over int2 (smallint) data types. It accumulates values by maintaining a running sum and count in an Int8TransTypeData structure stored within an array. The function performs in-place modification of the transition state when called in an aggregate context to optimize memory usage.

The function expects the transition state to be a 2-element int8 array containing an Int8TransTypeData structure with count and sum fields. For each new input value, it increments the count and adds the value to the running sum.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Arg 0: ArrayType* - The transition state array containing Int8TransTypeData
  - Arg 1: int16 - The new smallint value to accumulate

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT16
  - [Int8TransTypeData](../I/Int8TransTypeData.md)
  - [AggCheckCallContext](../A/AggCheckCallContext.md)  
  - PG_GETARG_ARRAYTYPE_P
  - PG_GETARG_ARRAYTYPE_P_COPY
  - ARR_HASNULL
  - ARR_SIZE
  - ARR_OVERHEAD_NONULLS
  - ARR_DATA_PTR
  - PG_RETURN_ARRAYTYPE_P
- Called from (representative examples):
  - No direct references found (used internally by PostgreSQL aggregate system)

## Notes and Other Information
- Optimizes memory allocation by modifying transition state in-place when called in aggregate context
- Validates transition array structure to ensure it contains expected Int8TransTypeData
- Part of PostgreSQL's aggregate function framework for computing averages over smallint columns
- The sum is maintained as int8 to prevent overflow when accumulating many int2 values
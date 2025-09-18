# int4_avg_accum_inv

## Location
[src/backend/utils/adt/numeric.c:6788-6815](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6788-L6815)

## Overview
PostgreSQL aggregate inverse transition function that removes int4 (integer) values from the average transition state, supporting sliding window aggregates.

## Definition
```c
Datum int4_avg_accum_inv(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the inverse transition function for AVG() aggregates over int4 (integer) data types in sliding window aggregate scenarios. When PostgreSQL needs to remove values from the beginning of a window while maintaining the aggregate state, this function decrements the count and subtracts the value from the running sum.

The function expects the same transition state format as int4_avg_accum - a 2-element int8 array containing an Int8TransTypeData structure. It performs the inverse operations: decrementing the count and subtracting the value from the sum.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Arg 0: ArrayType* - The transition state array containing Int8TransTypeData
  - Arg 1: int32 - The integer value to remove from accumulation

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_INT32
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
  - No direct references found (used internally by PostgreSQL windowing aggregate system)

## Notes and Other Information
- Inverse counterpart to int4_avg_accum, performing subtraction instead of addition  
- Nearly identical to int2_avg_accum_inv but handles int4 (integer) input values instead of int2 (smallint)
- Essential for efficient sliding window aggregates with OVER clauses and frame specifications
- Optimizes memory allocation by modifying transition state in-place when called in aggregate context
- Validates transition array structure to ensure it contains expected Int8TransTypeData
- Part of PostgreSQL's windowing aggregate framework for maintaining running averages in sliding windows
- Allows removal of values from the aggregate state without recalculating from scratch
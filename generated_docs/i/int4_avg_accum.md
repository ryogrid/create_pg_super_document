# int4_avg_accum

## Location
[src/backend/utils/adt/numeric.c:6701-6728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6701-L6728)

## Overview
PostgreSQL aggregate transition function that accumulates int4 (integer) values for computing the average, maintaining both a sum and count in an internal transition state.

## Definition
```c
Datum int4_avg_accum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the transition function for AVG() aggregates over int4 (integer) data types. It accumulates values by maintaining a running sum and count in an Int8TransTypeData structure stored within an array. The function performs in-place modification of the transition state when called in an aggregate context to optimize memory usage.

The function expects the transition state to be a 2-element int8 array containing an Int8TransTypeData structure with count and sum fields. For each new input value, it increments the count and adds the value to the running sum.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention
  - Arg 0: ArrayType* - The transition state array containing Int8TransTypeData
  - Arg 1: int32 - The new integer value to accumulate

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
  - No direct references found (used internally by PostgreSQL aggregate system)

## Notes and Other Information
- Nearly identical to int2_avg_accum but handles int4 (integer) input values instead of int2 (smallint)
- Optimizes memory allocation by modifying transition state in-place when called in aggregate context
- Validates transition array structure to ensure it contains expected Int8TransTypeData
- Part of PostgreSQL's aggregate function framework for computing averages over integer columns
- The sum is maintained as int8 to prevent overflow when accumulating many int4 values

## Simplified Source

```c
Datum int4_avg_accum(PG_FUNCTION_ARGS) {
    ArrayType *transarray;
    int32 newval = PG_GETARG_INT32(1);
    Int8TransTypeData *transdata;

    // Optimize: modify in-place for aggregate context, copy otherwise
    if (AggCheckCallContext(fcinfo, NULL))
        transarray = PG_GETARG_ARRAYTYPE_P(0);
    else
        transarray = PG_GETARG_ARRAYTYPE_P_COPY(0);

    // Validate transition array structure
    if (ARR_HASNULL(transarray) ||
        ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        elog(ERROR, "expected 2-element int8 array");

    // Update accumulator: increment count and add value to sum
    transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);
    transdata->count++;
    transdata->sum += newval;

    PG_RETURN_ARRAYTYPE_P(transarray);
}
```
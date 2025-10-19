# int2int4_sum

## Location
[src/backend/utils/adt/numeric.c:6843-6873](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6843-L6873)

## Overview
Computes the sum of int2 or int4 values by extracting the accumulated sum from an Int8TransTypeData transition array and returning it as an int8 result.

## Definition
```c
Datum int2int4_sum(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int2int4_sum` function is a PostgreSQL aggregate finalization function that extracts the final sum from transition data stored in an Int8TransTypeData array. This function serves as the final step for both SUM(int2) and SUM(int4) aggregate operations, since both return int8 results. It validates the input array structure, checks for empty input sets, and directly returns the accumulated sum value.

The function follows SQL standard behavior by returning NULL when no values were aggregated. Unlike the average function, it simply extracts the sum without performing any division operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `transarray`: ArrayType pointer to the transition data array containing Int8TransTypeData

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_ARRAYTYPE_P
  - ARR_HASNULL
  - ARR_SIZE
  - ARR_OVERHEAD_NONULLS
  - ARR_DATA_PTR
  - Int64GetDatumFast
  - PG_RETURN_DATUM
- Called from (representative examples):
  - No direct references found (likely registered as aggregate function)

## Notes and Other Information
- Used for both SUM(int2) and SUM(int4) aggregate operations
- Returns int8 type for both int2 and int4 input types to prevent overflow
- Returns NULL for empty input sets per SQL standard
- Simpler than int8_avg as it only extracts sum without division
- Uses Int64GetDatumFast for efficient int8 datum creation
- Validates array structure with specific size and null checks
- Part of PostgreSQL's integer sum aggregate system

## Simplified Source

```c
Datum int2int4_sum(PG_FUNCTION_ARGS) {
    // Get transition array containing accumulated data
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);

    // Validate array structure and extract transition data
    if (ARR_HASNULL(transarray) ||
        ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        elog(ERROR, "expected 2-element int8 array");

    Int8TransTypeData *transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);

    // Return NULL for empty input per SQL standard
    if (transdata->count == 0)
        PG_RETURN_NULL();

    // Return accumulated sum as int8
    PG_RETURN_DATUM(Int64GetDatumFast(transdata->sum));
}
```
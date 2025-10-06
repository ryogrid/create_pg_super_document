# int8_avg

## Location
[src/backend/utils/adt/numeric.c:6816-6842](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6816-L6842)

## Overview
Computes the average of integer values by performing division of the accumulated sum by the count from an Int8TransTypeData transition array.

## Definition
```c
Datum int8_avg(PG_FUNCTION_ARGS)
```

## Detailed Description
The `int8_avg` function is a PostgreSQL aggregate finalization function that calculates the average from transition data stored in an Int8TransTypeData array. This function is used as the final step in computing averages for int2 and int4 data types. It extracts the count and sum from the transition array, converts them to numeric values, and performs division to compute the final average result.

The function validates the input array structure and returns NULL if no values were aggregated (following SQL standard behavior for AVG of empty sets). The actual division is performed using PostgreSQL's numeric division function to ensure precision.

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
  - [int64_to_numeric](int64_to_numeric.md)
  - [NumericGetDatum](../N/NumericGetDatum.md)
  - [numeric_div](../n/numeric_div.md)
  - DirectFunctionCall2
  - PG_RETURN_DATUM
- Called from (representative examples):
  - No direct references found (likely registered as aggregate function)

## Notes and Other Information
- Used specifically for avg(int2) and avg(int4) aggregate operations
- Part of PostgreSQL's numeric aggregate system
- Returns NULL for empty input sets per SQL standard
- Uses numeric division for precise decimal results
- Validates array structure with specific size and null checks
- Transition data format: Int8TransTypeData with count and sum fields

## Simplified Source

```c
Datum int8_avg(PG_FUNCTION_ARGS) {
    ArrayType *transarray = PG_GETARG_ARRAYTYPE_P(0);

    // Validate array structure
    if (ARR_HASNULL(transarray) ||
        ARR_SIZE(transarray) != ARR_OVERHEAD_NONULLS(1) + sizeof(Int8TransTypeData))
        elog(ERROR, "expected 2-element int8 array");

    Int8TransTypeData *transdata = (Int8TransTypeData *) ARR_DATA_PTR(transarray);

    // Return NULL for empty set (SQL standard)
    if (transdata->count == 0)
        return PG_RETURN_NULL();

    // Convert sum and count to numeric, then divide for average
    Datum sum_numeric = NumericGetDatum(int64_to_numeric(transdata->sum));
    Datum count_numeric = NumericGetDatum(int64_to_numeric(transdata->count));

    return DirectFunctionCall2(numeric_div, sum_numeric, count_numeric);
}
```
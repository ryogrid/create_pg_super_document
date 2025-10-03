# numeric_avg

## Location
[src/backend/utils/adt/numeric.c:6144-6178](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6144-L6178)

## Overview
Computes the average (arithmetic mean) of accumulated numeric values during aggregate operations.

## Definition

```c
Datum
numeric_avg(PG_FUNCTION_ARGS)
```
## Detailed Description
This function is the final step of the numeric average aggregate operation. It takes the accumulated state from numeric aggregate operations and computes the final average by dividing the sum by the count of values. The function handles special cases including NULL inputs, NaN values, and positive/negative infinity values according to mathematical rules.

The function performs the following operations:
1. Validates the aggregate state and handles NULL cases
2. Checks for special numeric values (NaN, positive/negative infinity)
3. Extracts the sum and count from the aggregate state
4. Performs division to compute the average using numeric_div

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the aggregate state
## Dependencies
- Functions called/Symbols referenced:
  - [NumericAggState](../N/NumericAggState.md) (aggregate state structure)
  - NA_TOTAL_COUNT (macro to get total count from state)
  - [make_result](../m/make_result.md) (creates Numeric from NumericVar)
  - [int64_to_numeric](../i/int64_to_numeric.md) (converts int64 to Numeric)
  - [NumericGetDatum](../N/NumericGetDatum.md) (converts Numeric to Datum)
  - init_var (initializes NumericVar)
  - [accum_sum_final](../a/accum_sum_final.md) (finalizes accumulated sum)
  - [free_var](../f/free_var.md) (frees NumericVar memory)
  - [numeric_div](numeric_div.md) (performs numeric division)
  - DirectFunctionCall2 (calls PostgreSQL function directly)
- Called from (representative examples):
  - [numeric_poly_avg](numeric_poly_avg.md)

## Notes and Other Information
- Returns NULL if there were no non-null input values
- Returns NaN if any input was NaN or if both positive and negative infinities were present
- Returns positive infinity if only positive infinities were present
- Returns negative infinity if only negative infinities were present
- Uses numeric_div for the final division operation to ensure proper numeric precision
- Part of PostgreSQL's numeric aggregate function family
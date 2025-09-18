# numeric_avg

## Location
src/backend/utils/adt/numeric.c: 6144 - 6178

## Overview
Computes the average (arithmetic mean) of accumulated numeric values during aggregate operations.

## Definition


## Detailed Description
This function is the final step of the numeric average aggregate operation. It takes the accumulated state from numeric aggregate operations and computes the final average by dividing the sum by the count of values. The function handles special cases including NULL inputs, NaN values, and positive/negative infinity values according to mathematical rules.

The function performs the following operations:
1. Validates the aggregate state and handles NULL cases
2. Checks for special numeric values (NaN, positive/negative infinity)
3. Extracts the sum and count from the aggregate state
4. Performs division to compute the average using numeric_div

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing the aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - NumericAggState (aggregate state structure)
  - NA_TOTAL_COUNT (macro to get total count from state)
  - make_result (creates Numeric from NumericVar)
  - int64_to_numeric (converts int64 to Numeric)
  - NumericGetDatum (converts Numeric to Datum)
  - init_var (initializes NumericVar)
  - accum_sum_final (finalizes accumulated sum)
  - free_var (frees NumericVar memory)
  - numeric_div (performs numeric division)
  - DirectFunctionCall2 (calls PostgreSQL function directly)
- Called from (representative examples):
  - numeric_poly_avg

## Notes and Other Information
- Returns NULL if there were no non-null input values
- Returns NaN if any input was NaN or if both positive and negative infinities were present
- Returns positive infinity if only positive infinities were present
- Returns negative infinity if only negative infinities were present
- Uses numeric_div for the final division operation to ensure proper numeric precision
- Part of PostgreSQL's numeric aggregate function family
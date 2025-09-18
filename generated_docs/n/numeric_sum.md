# numeric_sum

## Location
src/backend/utils/adt/numeric.c: 6179 - 6221

## Overview
Computes the sum of accumulated numeric values during aggregate operations.

## Definition
```c
Datum numeric_sum(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is the final step of the numeric sum aggregate operation. It takes the accumulated state from numeric aggregate operations and returns the final sum of all processed values. The function handles special cases including NULL inputs, NaN values, and positive/negative infinity values according to mathematical rules.

The function performs the following operations:
1. Validates the aggregate state and handles NULL cases
2. Checks for special numeric values (NaN, positive/negative infinity)
3. Extracts the accumulated sum from the aggregate state
4. Finalizes the sum and returns it as a Numeric result

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - NumericAggState (aggregate state structure)
  - NA_TOTAL_COUNT (macro to get total count from state)
  - make_result (creates Numeric from NumericVar)
  - init_var (initializes NumericVar)
  - accum_sum_final (finalizes accumulated sum)
  - free_var (frees NumericVar memory)
- Called from (representative examples):
  - numeric_poly_sum

## Notes and Other Information
- Returns NULL if there were no non-null input values
- Returns NaN if any input was NaN or if both positive and negative infinities were present
- Returns positive infinity if only positive infinities were present
- Returns negative infinity if only negative infinities were present
- The accumulated sum is stored in the sumX field of the NumericAggState structure
- Part of PostgreSQL's numeric aggregate function family
- Similar to numeric_avg but returns the sum directly without division
# numeric_accum

## Location
src/backend/utils/adt/numeric.c: 5036 - 5055

## Overview
A PostgreSQL aggregate transition function for numeric aggregates that require both sum (sumX) and sum of squares (sumX2) calculations, such as variance and standard deviation functions.

## Definition


## Detailed Description
This function serves as the transition function for numeric aggregate operations that need to maintain both the sum of values and the sum of squared values. It's specifically designed for statistical aggregates like variance (VAR_SAMP, VAR_POP) and standard deviation (STDDEV_SAMP, STDDEV_POP) that require both sumX and sumX2 for their calculations.

The function follows PostgreSQL's standard aggregate function protocol:
- On the first call (when state is NULL), it initializes a new NumericAggState with calcSumX2=true
- For each subsequent call, it accumulates the input value into the existing state
- It properly handles NULL input values by checking PG_ARGISNULL(1)
- Returns the updated state pointer for the next transition call

The function delegates the actual numeric accumulation work to do_numeric_accum, which handles the complex logic of maintaining sums, decimal scales, and special values.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention where:
  - Argument 0: Current aggregate state (NumericAggState pointer, may be NULL on first call)
  - Argument 1: Input numeric value to accumulate (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - makeNumericAggState
  - PG_GETARG_NUMERIC
  - do_numeric_accum
  - PG_RETURN_POINTER
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's aggregate function catalog)

## Notes and Other Information
- This function is specifically for aggregates requiring sumX2 (sum of squares) calculations
- The calcSumX2=true parameter to makeNumericAggState distinguishes this from simpler sum-only aggregates
- Used internally by PostgreSQL's statistical aggregate functions for numeric types
- Follows PostgreSQL's memory management conventions for aggregate functions
- The function is registered in PostgreSQL's system catalog and called automatically during aggregate operations
- NULL input values are properly skipped without affecting the aggregate state
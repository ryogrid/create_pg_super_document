# numeric_avg_accum

## Location
[src/backend/utils/adt/numeric.c:5128-5147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L5128-L5147)

## Overview
A PostgreSQL aggregate transition function for numeric aggregates that only require sum (sumX) calculations, such as average (AVG) and sum (SUM) functions, without needing sum of squares.

## Definition

```c
Datum
numeric_avg_accum(PG_FUNCTION_ARGS)
```
## Detailed Description
This function serves as the transition function for simpler numeric aggregate operations that only need to maintain the sum of values, not sum of squares. It's specifically designed for aggregates like AVG (average) and SUM that don't require variance or standard deviation calculations.

The key difference from numeric_accum is that this function initializes the NumericAggState with calcSumX2=false, which means it won't maintain sumX2 (sum of squares), making it more efficient for operations that don't need this additional calculation.

The function follows PostgreSQL's standard aggregate function protocol:
- On the first call (when state is NULL), it initializes a new NumericAggState with calcSumX2=false  
- For each subsequent call, it accumulates the input value into the existing state
- It properly handles NULL input values by checking PG_ARGISNULL(1)
- Returns the updated state pointer for the next transition call

The actual numeric accumulation work is delegated to do_numeric_accum, which handles the complex logic of maintaining sums, decimal scales, and special values.

## Parameters / Member Variables
- Uses PostgreSQL's PG_FUNCTION_ARGS convention where:
  - Argument 0: Current aggregate state (NumericAggState pointer, may be NULL on first call)
  - Argument 1: Input numeric value to accumulate (may be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - PG_ARGISNULL
  - PG_GETARG_POINTER
  - [makeNumericAggState](../m/makeNumericAggState.md)
  - PG_GETARG_NUMERIC  
  - [do_numeric_accum](../d/do_numeric_accum.md)
  - PG_RETURN_POINTER
- Called from (representative examples):
  - No direct references found (likely referenced through PostgreSQL's aggregate function catalog)

## Notes and Other Information
- This function is specifically for aggregates that do NOT require sumX2 (sum of squares) calculations
- The calcSumX2=false parameter to makeNumericAggState is the key distinction from numeric_accum
- Used internally by PostgreSQL's simpler aggregate functions like AVG and SUM for numeric types
- More efficient than numeric_accum since it doesn't maintain sum of squares
- Follows PostgreSQL's memory management conventions for aggregate functions
- The function is registered in PostgreSQL's system catalog and called automatically during aggregate operations
- NULL input values are properly skipped without affecting the aggregate state
- Complements numeric_accum by providing an optimized path for simpler aggregates
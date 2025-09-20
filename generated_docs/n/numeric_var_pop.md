# numeric_var_pop

## Location
[src/backend/utils/adt/numeric.c:6340-6356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6340-L6356)

## Overview
Computes the population variance of numeric values from an aggregate state, providing the final result for the VAR_POP() aggregate function.

## Definition

```c
Datum
numeric_var_pop(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL aggregate function finalizer that calculates the population variance from accumulated numeric values. It takes a  pointer as input (which contains the accumulated sum, sum of squares, and count) and delegates to  to perform the actual variance calculation. The function returns the population variance, which differs from sample variance by using N (total count) rather than N-1 in the denominator.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro containing:
  - : NumericAggState pointer containing accumulated statistics (sum, sum of squares, count)

## Dependencies
- Functions called/Symbols referenced:
  -  (performs the actual variance/standard deviation calculation)
  -  (aggregate state structure)
  -  (PostgreSQL numeric data type)
  -  (macro for returning numeric values)
- Called from (representative examples):
  -  (polymorphic variant)

## Notes and Other Information
- This is a PostgreSQL aggregate function finalizer, called at the end of aggregation
- Returns NULL if the input state is NULL or if no valid data was accumulated
- Uses population variance formula (divides by N) rather than sample variance (divides by N-1)
- Part of PostgreSQL's statistical aggregate function family for numeric data types
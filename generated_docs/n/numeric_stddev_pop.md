# numeric_stddev_pop

## Location
[src/backend/utils/adt/numeric.c:6357-6374](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6357-L6374)

## Overview
Computes the population standard deviation of numeric values from an aggregate state, providing the final result for the STDDEV_POP() aggregate function.

## Definition

```c
Datum
numeric_stddev_pop(PG_FUNCTION_ARGS)
```
## Detailed Description
The `numeric_stddev_pop` function is a PostgreSQL aggregate function finalizer that calculates the population standard deviation from accumulated numeric values. It takes a `NumericAggState` pointer as input (which contains the accumulated sum, sum of squares, and count) and delegates to `numeric_stddev_internal` to perform the actual standard deviation calculation. The function returns the population standard deviation, which differs from sample standard deviation by using N (total count) rather than N-1 in the denominator of the variance calculation before taking the square root.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing:
  - `state`: NumericAggState pointer containing accumulated statistics (sum, sum of squares, count)

## Dependencies
- Functions called/Symbols referenced:
  - [numeric_stddev_internal](numeric_stddev_internal.md) (performs the actual variance/standard deviation calculation)
  - [NumericAggState](../N/NumericAggState.md) (aggregate state structure)
  - `Numeric` (PostgreSQL numeric data type)
  - `PG_RETURN_NUMERIC` (macro for returning numeric values)
- Called from (representative examples):
  - [numeric_poly_stddev_pop](numeric_poly_stddev_pop.md) (polymorphic variant)

## Notes and Other Information
- This is a PostgreSQL aggregate function finalizer, called at the end of aggregation
- Returns NULL if the input state is NULL or if no valid data was accumulated
- Uses population standard deviation formula (square root of variance divided by N) rather than sample standard deviation (square root of variance divided by N-1)
- Part of PostgreSQL's statistical aggregate function family for numeric data types
- The actual computation is delegated to `numeric_stddev_internal` with parameters indicating population (not sample) and standard deviation (not variance)
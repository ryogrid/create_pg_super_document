# numeric_stddev_samp

## Location
[src/backend/utils/adt/numeric.c:6323-6339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6323-L6339)

## Overview
Computes the sample standard deviation of accumulated numeric values during aggregate operations.

## Definition
```c
Datum numeric_stddev_samp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the final step for the sample standard deviation aggregate operation in PostgreSQL. It acts as a wrapper around the numeric_stddev_internal function, specifically requesting sample standard deviation calculation. Sample standard deviation uses N-1 in the denominator (Bessel's correction) and takes the square root of the sample variance to provide an unbiased estimate of population standard deviation.

The function performs the following operations:
1. Extracts the aggregate state from the function arguments
2. Calls numeric_stddev_internal with variance=false and sample=true
3. Returns the computed sample standard deviation or NULL if undefined

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - [NumericAggState](../N/NumericAggState.md) (aggregate state structure)
  - [numeric_stddev_internal](numeric_stddev_internal.md) (core variance/stddev computation function)
- Called from (representative examples):
  - [numeric_poly_stddev_samp](numeric_poly_stddev_samp.md)

## Notes and Other Information
- Returns NULL when there are fewer than 2 non-null input values (sample standard deviation is mathematically undefined for N <= 1)
- Uses Bessel's correction (N-1 denominator) to provide unbiased estimation
- Computes the square root of sample variance to get standard deviation
- Part of PostgreSQL's statistical aggregate function family
- Implemented as a thin wrapper around the more general numeric_stddev_internal function
- Corresponds to the SQL STDDEV_SAMP() or STDDEV() aggregate function for numeric types
- The result has appropriate precision maintained through the underlying numeric system
- Standard deviation has the same units as the original data (unlike variance)

## Simplified Source

```c
Datum numeric_stddev_samp(PG_FUNCTION_ARGS) {
    NumericAggState *state;
    Numeric result;
    bool is_null;

    // Extract aggregate state from function arguments
    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    // Calculate sample standard deviation (variance=false, sample=true)
    result = numeric_stddev_internal(state, false, true, &is_null);

    if (is_null)
        PG_RETURN_NULL();
    else
        PG_RETURN_NUMERIC(result);
}
```
# numeric_var_samp

## Location
[src/backend/utils/adt/numeric.c:6306-6322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L6306-L6322)

## Overview
Computes the sample variance of accumulated numeric values during aggregate operations.

## Definition
```c
Datum numeric_var_samp(PG_FUNCTION_ARGS)
```

## Detailed Description
This function serves as the final step for the sample variance aggregate operation in PostgreSQL. It acts as a wrapper around the numeric_stddev_internal function, specifically requesting sample variance calculation. Sample variance uses N-1 in the denominator (Bessel's correction) to provide an unbiased estimate of population variance.

The function performs the following operations:
1. Extracts the aggregate state from the function arguments
2. Calls numeric_stddev_internal with variance=true and sample=true
3. Returns the computed sample variance or NULL if undefined

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro containing the aggregate state

## Dependencies
- Functions called/Symbols referenced:
  - [NumericAggState](../N/NumericAggState.md) (aggregate state structure)
  - [numeric_stddev_internal](numeric_stddev_internal.md) (core variance/stddev computation function)
- Called from (representative examples):
  - [numeric_poly_var_samp](numeric_poly_var_samp.md)

## Notes and Other Information
- Returns NULL when there are fewer than 2 non-null input values (sample variance is mathematically undefined for N <= 1)
- Uses Bessel's correction (N-1 denominator) to provide unbiased estimation
- Part of PostgreSQL's statistical aggregate function family
- Implemented as a thin wrapper around the more general numeric_stddev_internal function
- Corresponds to the SQL VAR_SAMP() aggregate function for numeric types
- The result has appropriate precision maintained through the underlying numeric system

## Simplified Source

```c
Datum numeric_var_samp(PG_FUNCTION_ARGS) {
    NumericAggState *state;
    Numeric result;
    bool is_null;

    // Extract aggregate state from function arguments
    state = PG_ARGISNULL(0) ? NULL : (NumericAggState *) PG_GETARG_POINTER(0);

    // Calculate sample variance (variance=true, sample=true)
    result = numeric_stddev_internal(state, true, true, &is_null);

    if (is_null)
        PG_RETURN_NULL();
    else
        PG_RETURN_NUMERIC(result);
}
```
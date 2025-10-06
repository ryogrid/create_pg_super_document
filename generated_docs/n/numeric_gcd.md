# numeric_gcd

## Location
[src/backend/utils/adt/numeric.c:3537-3579](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3537-L3579)

## Overview
Calculates the greatest common divisor (GCD) of two numeric values using the Euclidean algorithm.

## Definition
```c
Datum numeric_gcd(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_gcd` function implements the mathematical greatest common divisor operation for PostgreSQL numeric data types. It takes two numeric arguments and returns their GCD using the Euclidean algorithm. The function handles special cases including NaN and infinity values by returning NaN for any input containing special values. The core computation is performed using internal numeric variables and the `gcd_var()` helper function to ensure precision and efficiency.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `num1` (Numeric): First numeric value for GCD calculation
  - `num2` (Numeric): Second numeric value for GCD calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC`: Extracts numeric arguments from function call context
  - `NUMERIC_IS_SPECIAL`: Checks for NaN and infinity values
  - [make_result](../m/make_result.md): Converts NumericVar to Numeric result
  - [init_var_from_num](../i/init_var_from_num.md): Initializes NumericVar from Numeric input
  - `init_var`: Initializes empty NumericVar for result
  - [gcd_var](../g/gcd_var.md): Performs actual GCD computation on NumericVar types
  - [free_var](../f/free_var.md): Releases memory allocated for NumericVar
  - `PG_RETURN_NUMERIC`: Returns numeric result to caller
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- Part of the "Advanced math functions" section in PostgreSQL numeric implementation
- Returns NaN for any input containing NaN or infinity values
- Uses internal NumericVar representation for precise arithmetic operations
- Implements the mathematically standard Euclidean algorithm for GCD calculation
- Memory management includes proper cleanup of temporary NumericVar structures
- Located in src/backend/utils/adt/numeric.c alongside other advanced mathematical functions

## Simplified Source

```c
Datum
numeric_gcd(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1, arg2, result;

    // Handle special values (NaN, infinity) - return NaN
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
        PG_RETURN_NUMERIC(make_result(&const_nan));

    // Convert inputs to internal numeric variables
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    // Compute GCD using Euclidean algorithm
    gcd_var(&arg1, &arg2, &result);

    // Convert result back and cleanup
    Numeric res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
```
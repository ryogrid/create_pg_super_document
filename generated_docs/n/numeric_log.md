# numeric_log

## Location
[src/backend/utils/adt/numeric.c:3880-3950](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3880-L3950)

## Overview
Computes the logarithm of one numeric value using another numeric value as the base, with comprehensive special value handling and mathematical constraint validation.

## Definition

```c
Datum
numeric_log(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function calculates log_base2(value1) where the first argument is the value and the second argument is the base. It implements comprehensive special value semantics: log(∞, ∞) returns NaN due to the indeterminate form ∞/∞, log(∞, finite) returns 0, and log(finite, ∞) returns ∞. The function enforces mathematical constraints by rejecting negative inputs and zero inputs with appropriate error messages.

The implementation handles all combinations of special numeric values (NaN, ±∞) according to mathematical conventions. For finite inputs, it delegates the actual logarithm computation to , which handles scale selection internally. This design separates special value handling from the core mathematical computation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing two numeric values (value and base)
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extract first and second numeric arguments
  - NUMERIC_IS_SPECIAL: Check if either input is NaN or infinity
  - NUMERIC_IS_NAN: Check for NaN values in either input
  - [make_result](../m/make_result.md): Convert constant results to Numeric
  - const_nan: Constant NaN NumericVar for indeterminate results
  - [numeric_sign_internal](numeric_sign_internal.md): Get sign of numeric values
  - const_zero: Constant zero NumericVar for log(∞, finite) case
  - const_pinf: Constant positive infinity NumericVar for log(finite, ∞) case
  - NUMERIC_IS_PINF: Check for positive infinity values
  - [init_var_from_num](../i/init_var_from_num.md): Initialize NumericVar from both inputs
  - init_var: Initialize result NumericVar
  - [log_var](../l/log_var.md): Core logarithm calculation function
  - [free_var](../f/free_var.md): Free NumericVar memory
  - PG_RETURN_NUMERIC: Return numeric result
- Called from (representative examples):
  - SQL log() function calls with two arguments
  - Logarithmic expressions with custom bases

## Notes and Other Information
- Raises  for negative or zero inputs
- Implements special case: log(∞, ∞) = NaN due to indeterminate form
- Returns zero for log(∞, finite-positive) without underflow error
- Returns infinity for log(finite-positive, ∞)
- Scale selection handled internally by  function
- Validates both arguments before computation
- Located in

## Simplified Source

```c
Datum
numeric_log(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);  // value
    Numeric num2 = PG_GETARG_NUMERIC(1);  // base
    Numeric res;
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;

    // Handle special values (NaN, infinity)
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2)) {
        int sign1, sign2;

        if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
            PG_RETURN_NUMERIC(make_result(&const_nan));

        // Check for negative inputs (log undefined for negative numbers)
        sign1 = numeric_sign_internal(num1);
        sign2 = numeric_sign_internal(num2);
        if (sign1 < 0 || sign2 < 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                           errmsg("cannot take logarithm of a negative number")));

        // Check for zero inputs (log undefined for zero)
        if (sign1 == 0 || sign2 == 0)
            ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_LOG),
                           errmsg("cannot take logarithm of zero")));

        // Handle infinity cases
        if (NUMERIC_IS_PINF(num1)) {
            // log(Inf, Inf) = NaN (indeterminate form)
            if (NUMERIC_IS_PINF(num2))
                PG_RETURN_NUMERIC(make_result(&const_nan));
            // log(Inf, finite) = 0
            PG_RETURN_NUMERIC(make_result(&const_zero));
        }
        // log(finite, Inf) = Inf
        PG_RETURN_NUMERIC(make_result(&const_pinf));
    }

    // Initialize variables for finite inputs
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    // Perform logarithm calculation (handles scale internally)
    log_var(&arg1, &arg2, &result);

    // Create and return result
    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
```
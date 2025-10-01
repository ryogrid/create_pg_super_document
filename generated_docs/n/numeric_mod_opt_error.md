# numeric_mod_opt_error

## Location
[src/backend/utils/adt/numeric.c:3384-3452](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3384-L3452)

## Overview
Internal version of numeric modulo operation that provides optional error handling, allowing callers to handle modulo errors (like division by zero) without raising exceptions.

## Definition
```c
Numeric numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
```

## Detailed Description
This function performs modulo (remainder) operation on two PostgreSQL Numeric values with optional error handling. Unlike the standard `numeric_mod()` function, this variant allows the caller to handle errors gracefully by setting a flag instead of throwing exceptions. The function implements modulo semantics similar to POSIX fmod() with PostgreSQL-specific handling for special numeric values.

Key behaviors:
- Returns NULL and sets `*have_error = true` when division by zero occurs (if error handling is enabled)
- Handles special cases: NaN propagation, infinity modulo rules following POSIX fmod()
- Returns NaN for infinity % finite_nonzero cases
- Returns original dividend when divisor is infinity
- Uses mod_var for the core modulo computation

## Parameters / Member Variables
- `num1`: The dividend (numerator) - the Numeric value to find remainder for
- `num2`: The divisor (denominator) - the Numeric value to divide by for remainder
- `have_error`: Optional pointer to bool flag; if provided, set to true on error instead of throwing exception

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN, NUMERIC_IS_INF
  - [make_result](../m/make_result.md), make_result_opt_error
  - [numeric_sign_internal](numeric_sign_internal.md), duplicate_numeric
  - [init_var_from_num](../i/init_var_from_num.md), init_var, free_var
  - [mod_var](../m/mod_var.md) (core modulo implementation)
- Called from (representative examples):
  - [numeric_mod](numeric_mod.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (JSON path execution)

## Notes and Other Information
- This is the core implementation function for numeric modulo operations in PostgreSQL
- Follows POSIX fmod() semantics for special value handling with PostgreSQL adaptations
- Differs from POSIX by only raising division by zero error for y-is-zero, not x-is-infinite
- Error handling mechanism allows for more robust numeric computations in complex expressions
- Used internally by higher-level modulo functions and JSON path operations

## Simplified Source

```c
Numeric
numeric_mod_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
    Numeric res;
    NumericVar arg1, arg2, result;

    if (have_error)
        *have_error = false;

    // Handle special values (NaN, infinity) following POSIX fmod() semantics
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2)) {
        // NaN propagates
        if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
            return make_result(&const_nan);

        if (NUMERIC_IS_INF(num1)) {
            // Check for division by zero
            if (numeric_sign_internal(num2) == 0) {
                if (have_error) {
                    *have_error = true;
                    return NULL;
                }
                ereport(ERROR, "division by zero");
            }
            // Infinity % any_nonzero = NaN
            return make_result(&const_nan);
        }

        // num2 is infinity: finite % infinity = finite
        return duplicate_numeric(num1);
    }

    // Normal modulo: convert to internal format
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    // Check for division by zero if error handling enabled
    if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0)) {
        *have_error = true;
        return NULL;
    }

    // Perform the modulo operation
    mod_var(&arg1, &arg2, &result);

    res = make_result_opt_error(&result, NULL);
    free_var(&result);

    return res;
}
```
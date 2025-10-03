# numeric_div_opt_error

## Location
[src/backend/utils/adt/numeric.c:3160-3274](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3160-L3274)

## Overview
Internal version of numeric division that provides optional error handling, allowing callers to handle division errors (like division by zero) without raising exceptions.

## Definition

```c
Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
```
## Detailed Description
This function performs division of two PostgreSQL Numeric values with optional error handling. Unlike the standard  function, this variant allows the caller to handle errors gracefully by setting a flag instead of throwing exceptions. The function handles special numeric values (NaN, positive/negative infinity) according to IEEE standards and PostgreSQL conventions.

Key behaviors:
- Returns NULL and sets  when division by zero occurs (if error handling is enabled)
- Handles special cases: NaN propagation, infinity division rules
- Uses configurable scale selection for the result precision
- Implements IEEE-compliant special value arithmetic

## Parameters / Member Variables
- `num1`: The dividend (numerator) - the Numeric value to be divided
- `num2`: The divisor (denominator) - the Numeric value to divide by
- `*have_error`: Optional pointer to bool flag; if provided, set to true on error instead of throwing exception
## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN, NUMERIC_IS_PINF, NUMERIC_IS_NINF
  - [make_result](../m/make_result.md), make_result_opt_error
  - [numeric_sign_internal](numeric_sign_internal.md)
  - [init_var_from_num](../i/init_var_from_num.md), init_var, free_var
  - [select_div_scale](../s/select_div_scale.md), div_var
- Called from (representative examples):
  - [numeric_div](numeric_div.md)
  - [executeItemOptUnwrapTarget](../e/executeItemOptUnwrapTarget.md) (JSON path execution)
  - [timestamp_part_common](../t/timestamp_part_common.md), timestamptz_part_common

## Notes and Other Information
- This is the core implementation function for numeric division operations in PostgreSQL
- Follows IEEE 754 standards for special value handling (NaN, infinity)
- The scale selection algorithm ensures appropriate precision for division results
- Used internally by higher-level division functions and timestamp arithmetic
- Error handling mechanism allows for more robust numeric computations in complex expressions

## Simplified Source

```c
Numeric
numeric_div_opt_error(Numeric num1, Numeric num2, bool *have_error)
{
    NumericVar arg1, arg2, result;
    Numeric res;
    int rscale;

    if (have_error)
        *have_error = false;

    // Handle special values (NaN, infinity)
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2)) {
        // NaN propagates
        if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
            return make_result(&const_nan);

        // Handle infinity division
        if (NUMERIC_IS_PINF(num1)) {
            if (NUMERIC_IS_SPECIAL(num2))
                return make_result(&const_nan);  // Inf / Inf = NaN

            // Check divisor sign for +Inf / finite
            int sign = numeric_sign_internal(num2);
            if (sign == 0) {  // Division by zero
                if (have_error) {
                    *have_error = true;
                    return NULL;
                }
                // Throw division by zero error
            }
            return (sign > 0) ? make_result(&const_pinf) : make_result(&const_ninf);
        }

        if (NUMERIC_IS_NINF(num1)) {
            if (NUMERIC_IS_SPECIAL(num2))
                return make_result(&const_nan);  // -Inf / Inf = NaN

            // Check divisor sign for -Inf / finite
            int sign = numeric_sign_internal(num2);
            if (sign == 0) {  // Division by zero
                if (have_error) {
                    *have_error = true;
                    return NULL;
                }
                // Throw division by zero error
            }
            return (sign > 0) ? make_result(&const_ninf) : make_result(&const_pinf);
        }

        // finite / infinity = 0
        return make_result(&const_zero);
    }

    // Normal division: convert to internal format
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    // Select appropriate scale for result precision
    rscale = select_div_scale(&arg1, &arg2);

    // Check for division by zero if error handling enabled
    if (have_error && (arg2.ndigits == 0 || arg2.digits[0] == 0)) {
        *have_error = true;
        return NULL;
    }

    // Perform the division
    div_var(&arg1, &arg2, &result, rscale, true);

    res = make_result_opt_error(&result, have_error);
    free_var(&result);

    return res;
}
```
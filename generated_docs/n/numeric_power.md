# numeric_power

## Location
[src/backend/utils/adt/numeric.c:3951-4137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3951-L4137)

## Overview
Computes x raised to the power of y (x^y) with extensive special value handling following POSIX pow(3) specifications and SQL standards.

## Definition

```c
Datum
numeric_power(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements exponentiation for PostgreSQL numeric types with comprehensive handling of special values including NaN and infinities. It strictly follows POSIX pow(3) semantics: NaN^0 = 1, 1^NaN = 1, while other NaN combinations return NaN. The function implements detailed rules for infinity combinations, such as |x|<1 and y=±∞, |x|>1 and y=±∞, and special cases like (-1)^∞ = 1.

Mathematical constraints are enforced by raising appropriate errors for undefined operations like 0^(negative) and negative^(non-integer). The function validates that negative bases are only raised to integral powers to avoid complex results. For finite inputs, computation is delegated to  which handles scale selection internally.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing two numeric values (base and exponent)
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extract base and exponent numeric arguments
  - NUMERIC_IS_SPECIAL: Check if either input is NaN or infinity
  - NUMERIC_IS_NAN: Check for NaN values in either input
  - [init_var_from_num](../i/init_var_from_num.md): Initialize NumericVar from inputs
  - [cmp_var](../c/cmp_var.md): Compare NumericVar values with constants
  - [make_result](../m/make_result.md): Convert constant NumericVar to Numeric result
  - const_zero, const_one, const_nan, const_pinf, const_ninf, const_minus_one: Constant NumericVar values
  - [numeric_sign_internal](numeric_sign_internal.md): Get sign of numeric values
  - [numeric_is_integral](numeric_is_integral.md): Check if exponent is an integer
  - NUMERIC_IS_INF, NUMERIC_IS_PINF, NUMERIC_IS_NINF: Check for infinity types
  - NUMERIC_POS: Positive sign constant for absolute value calculation
  - init_var: Initialize result NumericVar
  - [power_var](../p/power_var.md): Core exponentiation calculation function
  - [free_var](../f/free_var.md): Free NumericVar memory
  - PG_RETURN_NUMERIC: Return numeric result
- Called from (representative examples):
  - [numeric_to_number](numeric_to_number.md): In formatting functions at src/backend/utils/adt/formatting.c:6385
  - [numeric_to_char](numeric_to_char.md): In formatting functions at src/backend/utils/adt/formatting.c:6475
  - SQL power() or ^ operator expressions

## Notes and Other Information
- Raises  for undefined operations
- Implements complete POSIX pow(3) special value semantics
- Handles complex mathematical edge cases: (-1)^∞ = 1, 0^(negative) = undefined
- Validates negative base with non-integer exponent to prevent complex results
- Uses bit manipulation to detect odd integers for sign preservation
- Scale selection handled internally by  function
- Located in

## Simplified Source

```c
Datum
numeric_power(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);  // base
    Numeric num2 = PG_GETARG_NUMERIC(1);  // exponent
    Numeric res;
    NumericVar arg1;
    NumericVar arg2;
    NumericVar result;
    int sign1, sign2;

    // Handle special values (NaN, infinity) with POSIX semantics
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2)) {
        // NaN^0 = 1, 1^NaN = 1, otherwise NaN for NaN inputs
        if (NUMERIC_IS_NAN(num1)) {
            if (!NUMERIC_IS_SPECIAL(num2)) {
                init_var_from_num(num2, &arg2);
                if (cmp_var(&arg2, &const_zero) == 0)
                    PG_RETURN_NUMERIC(make_result(&const_one));
            }
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }
        if (NUMERIC_IS_NAN(num2)) {
            if (!NUMERIC_IS_SPECIAL(num1)) {
                init_var_from_num(num1, &arg1);
                if (cmp_var(&arg1, &const_one) == 0)
                    PG_RETURN_NUMERIC(make_result(&const_one));
            }
            PG_RETURN_NUMERIC(make_result(&const_nan));
        }

        // Check for undefined operations
        sign1 = numeric_sign_internal(num1);
        sign2 = numeric_sign_internal(num2);
        if (sign1 == 0 && sign2 < 0)  // 0^(negative) = undefined
            ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                           errmsg("zero raised to a negative power is undefined")));
        if (sign1 < 0 && !numeric_is_integral(num2))  // negative^(non-integer) = complex
            ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                           errmsg("a negative number raised to a non-integer power yields a complex result")));

        // Handle specific cases: 1^anything = 1, anything^0 = 1
        if (!NUMERIC_IS_SPECIAL(num1)) {
            init_var_from_num(num1, &arg1);
            if (cmp_var(&arg1, &const_one) == 0)
                PG_RETURN_NUMERIC(make_result(&const_one));
        }
        if (sign2 == 0)
            PG_RETURN_NUMERIC(make_result(&const_one));
        if (sign1 == 0 && sign2 > 0)
            PG_RETURN_NUMERIC(make_result(&const_zero));

        // Handle infinity cases (simplified)
        if (NUMERIC_IS_INF(num2)) {
            bool abs_x_gt_one;
            if (NUMERIC_IS_SPECIAL(num1)) {
                abs_x_gt_one = true;
            } else {
                init_var_from_num(num1, &arg1);
                if (cmp_var(&arg1, &const_minus_one) == 0)
                    PG_RETURN_NUMERIC(make_result(&const_one));
                arg1.sign = NUMERIC_POS;  // abs(x)
                abs_x_gt_one = (cmp_var(&arg1, &const_one) > 0);
            }
            if (abs_x_gt_one == (sign2 > 0))
                PG_RETURN_NUMERIC(make_result(&const_pinf));
            else
                PG_RETURN_NUMERIC(make_result(&const_zero));
        }

        // Handle base infinity cases
        if (NUMERIC_IS_PINF(num1)) {
            if (sign2 > 0)
                PG_RETURN_NUMERIC(make_result(&const_pinf));
            else
                PG_RETURN_NUMERIC(make_result(&const_zero));
        }
        if (NUMERIC_IS_NINF(num1)) {
            if (sign2 < 0)
                PG_RETURN_NUMERIC(make_result(&const_zero));
            // Check if exponent is odd integer
            init_var_from_num(num2, &arg2);
            if (arg2.ndigits > 0 && arg2.ndigits == arg2.weight + 1 &&
                (arg2.digits[arg2.ndigits - 1] & 1))
                PG_RETURN_NUMERIC(make_result(&const_ninf));
            else
                PG_RETURN_NUMERIC(make_result(&const_pinf));
        }
    }

    // Additional validation for finite inputs
    sign1 = numeric_sign_internal(num1);
    sign2 = numeric_sign_internal(num2);
    if (sign1 == 0 && sign2 < 0)
        ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                       errmsg("zero raised to a negative power is undefined")));

    // Initialize variables and perform power calculation
    init_var(&result);
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);

    // Perform power calculation (handles scale internally)
    power_var(&arg1, &arg2, &result);

    // Create and return result
    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
```
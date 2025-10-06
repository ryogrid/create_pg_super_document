# numeric_div_trunc

## Location
[src/backend/utils/adt/numeric.c:3275-3363](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3275-L3363)

## Overview
PostgreSQL function that performs division of two numeric values and truncates the result to an integer, effectively implementing floor division.

## Definition
```c
Datum numeric_div_trunc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements truncated division (floor division) for PostgreSQL's Numeric data type. It divides the first argument by the second and truncates the result to an integer by calling div_var with scale 0 and round=false. The function handles special numeric values (NaN, infinity) according to IEEE standards and PostgreSQL conventions.

Key behaviors:
- Performs division and truncates result to integer (no fractional part)
- Handles special cases: NaN propagation, infinity division rules
- Throws division by zero errors when appropriate
- Returns integer result as Numeric type
- Uses PostgreSQL's function calling convention (PG_FUNCTION_ARGS)

## Parameters / Member Variables
- Function arguments accessed via PG_GETARG_NUMERIC():
  - Argument 0: The dividend (numerator) - the Numeric value to be divided
  - Argument 1: The divisor (denominator) - the Numeric value to divide by

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC, PG_RETURN_NUMERIC
  - NUMERIC_IS_SPECIAL, NUMERIC_IS_NAN, NUMERIC_IS_PINF, NUMERIC_IS_NINF
  - [make_result](../m/make_result.md), numeric_sign_internal
  - [init_var_from_num](../i/init_var_from_num.md), init_var, free_var
  - [div_var](../d/div_var.md) (called with scale=0, round=false for truncation)
- Called from (representative examples):
  - [numeric_half_rounded](numeric_half_rounded.md)
  - [numeric_truncated_divide](numeric_truncated_divide.md)

## Notes and Other Information
- This is a PostgreSQL built-in function accessible via SQL
- The truncation behavior differs from regular division which preserves decimal precision
- Used in database size calculation functions for converting between units
- Special value handling follows the same rules as regular numeric division
- The result is always an integer value represented as Numeric type

## Simplified Source

```c
Datum
numeric_div_trunc(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1, arg2, result;

    // Handle special values (NaN, infinity)
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2)) {
        if (NUMERIC_IS_NAN(num1) || NUMERIC_IS_NAN(num2))
            PG_RETURN_NUMERIC(make_result(&const_nan));

        // Handle infinity cases with appropriate sign logic
        if (NUMERIC_IS_PINF(num1) || NUMERIC_IS_NINF(num1)) {
            // Division by zero check and sign-based infinity return
            switch (numeric_sign_internal(num2)) {
                case 0: ereport(ERROR, (errcode(ERRCODE_DIVISION_BY_ZERO)));
                case 1: PG_RETURN_NUMERIC(make_result(NUMERIC_IS_PINF(num1) ? &const_pinf : &const_ninf));
                case -1: PG_RETURN_NUMERIC(make_result(NUMERIC_IS_PINF(num1) ? &const_ninf : &const_pinf));
            }
        }
        // Finite / Infinity = 0
        PG_RETURN_NUMERIC(make_result(&const_zero));
    }

    // Regular division with truncation
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    // Divide with scale=0 and round=false for truncation
    div_var(&arg1, &arg2, &result, 0, false);

    Numeric res = make_result(&result);
    free_var(&result);
    PG_RETURN_NUMERIC(res);
}
```
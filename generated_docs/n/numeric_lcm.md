# numeric_lcm

## Location
[src/backend/utils/adt/numeric.c:3580-3639](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3580-L3639)

## Overview
Calculates the least common multiple (LCM) of two numeric values using the mathematical relationship LCM(x,y) = abs(x / GCD(x,y) * y).

## Definition
```c
Datum numeric_lcm(PG_FUNCTION_ARGS)
```

## Detailed Description
The `numeric_lcm` function implements the mathematical least common multiple operation for PostgreSQL numeric data types. It computes LCM using the standard mathematical formula: LCM(x, y) = abs(x / GCD(x, y) * y). The function handles special cases including NaN and infinity values by returning NaN, and zero inputs by returning zero. The implementation ensures that the division by GCD is exact (returning an integer), making the LCM an integral multiple of both inputs. The results display scale is set to the maximum of the input scales to preserve precision formatting.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `num1` (Numeric): First numeric value for LCM calculation
  - `num2` (Numeric): Second numeric value for LCM calculation

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_NUMERIC`: Extracts numeric arguments from function call context
  - `NUMERIC_IS_SPECIAL`: Checks for NaN and infinity values
  - `make_result`: Converts NumericVar to Numeric result
  - `init_var_from_num`: Initializes NumericVar from Numeric input
  - `init_var`: Initializes empty NumericVar for result
  - `gcd_var`: Computes greatest common divisor
  - `div_var`, `mul_var`: Division and multiplication operations
  - `free_var`: Releases memory allocated for NumericVar
  - `PG_RETURN_NUMERIC`: Returns numeric result to caller

## Notes and Other Information
- Part of the "Advanced math functions" section in PostgreSQL numeric implementation
- Returns NaN for any input containing NaN or infinity values
- Uses LCM(x,y) = abs(x / GCD(x,y) * y) formula
- Returns zero if either input is zero
- Memory management includes proper cleanup of temporary NumericVar structures

## Simplified Source

```c
Datum
numeric_lcm(PG_FUNCTION_ARGS)
{
    Numeric num1 = PG_GETARG_NUMERIC(0);
    Numeric num2 = PG_GETARG_NUMERIC(1);
    NumericVar arg1, arg2, result;

    // Handle special values (NaN, infinity) - return NaN
    if (NUMERIC_IS_SPECIAL(num1) || NUMERIC_IS_SPECIAL(num2))
        PG_RETURN_NUMERIC(make_result(&const_nan));

    // Convert inputs to internal format
    init_var_from_num(num1, &arg1);
    init_var_from_num(num2, &arg2);
    init_var(&result);

    // Handle zero inputs - LCM is 0 if either input is 0
    if (arg1.ndigits == 0 || arg2.ndigits == 0) {
        set_var_from_var(&const_zero, &result);
    } else {
        // Compute LCM = abs(x / gcd(x,y) * y)
        gcd_var(&arg1, &arg2, &result);        // result = gcd(x,y)
        div_var(&arg1, &result, &result, 0, false);  // result = x / gcd(x,y)
        mul_var(&arg2, &result, &result, arg2.dscale); // result = y * (x / gcd(x,y))
        result.sign = NUMERIC_POS;             // Ensure positive result
    }

    // Set display scale and return result
    result.dscale = Max(arg1.dscale, arg2.dscale);
    Numeric res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
```
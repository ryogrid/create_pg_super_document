# numeric_sqrt

## Location
[src/backend/utils/adt/numeric.c:3692-3763](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L3692-L3763)

## Overview
Computes the square root of a numeric value with appropriate scale handling and special value processing.

## Definition

```c
Datum
numeric_sqrt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function calculates the square root of a PostgreSQL numeric data type. It handles special numeric values (NaN, positive and negative infinity) according to mathematical conventions. For negative infinity, it raises an error since square roots of negative numbers are undefined in real arithmetic. For NaN and positive infinity, it returns the same special value.

The function carefully determines the appropriate result scale to ensure at least  significant digits while respecting the input's decimal scale. It uses an optimized weight calculation that accounts for whether  is even or odd to minimize computational overhead.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing the input numeric value
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extract numeric argument from function args
  - NUMERIC_IS_SPECIAL: Check if numeric is NaN or infinity
  - NUMERIC_IS_NINF: Check if numeric is negative infinity
  - [duplicate_numeric](../d/duplicate_numeric.md): Create copy of numeric value
  - [init_var_from_num](../i/init_var_from_num.md): Initialize NumericVar from Numeric
  - init_var: Initialize empty NumericVar
  - [sqrt_var](../s/sqrt_var.md): Core square root calculation function
  - [make_result](../m/make_result.md): Convert NumericVar to Numeric result
  - [free_var](../f/free_var.md): Free NumericVar memory
  - PG_RETURN_NUMERIC: Return numeric result
- Called from (representative examples):
  - SQL sqrt() function calls
  - [Numeric](../N/Numeric.md) operator expressions

## Notes and Other Information
- Raises  error for negative infinity inputs
- Scale calculation optimizes for even  values to avoid rounding operations
- [Result](../R/Result.md) scale is bounded by  and 
- Uses  for the actual mathematical computation
- Located in

## Simplified Source

```c
Datum
numeric_sqrt(PG_FUNCTION_ARGS)
{
    Numeric num = PG_GETARG_NUMERIC(0);
    Numeric res;
    NumericVar arg;
    NumericVar result;
    int sweight;
    int rscale;

    // Handle special values (NaN, infinity)
    if (NUMERIC_IS_SPECIAL(num)) {
        if (NUMERIC_IS_NINF(num))
            ereport(ERROR, (errcode(ERRCODE_INVALID_ARGUMENT_FOR_POWER_FUNCTION),
                           errmsg("cannot take square root of a negative number")));
        // Return NaN or positive infinity as-is
        PG_RETURN_NUMERIC(duplicate_numeric(num));
    }

    // Initialize variables
    init_var_from_num(num, &arg);
    init_var(&result);

    // Calculate result scale based on input weight
    // Ensure at least NUMERIC_MIN_SIG_DIGITS significant digits
#if DEC_DIGITS == ((DEC_DIGITS / 2) * 2)
    sweight = arg.weight * DEC_DIGITS / 2 + 1;
#else
    if (arg.weight >= 0)
        sweight = arg.weight * DEC_DIGITS / 2 + 1;
    else
        sweight = 1 - (1 - arg.weight * DEC_DIGITS) / 2;
#endif

    // Determine appropriate result scale
    rscale = NUMERIC_MIN_SIG_DIGITS - sweight;
    rscale = Max(rscale, arg.dscale);
    rscale = Max(rscale, NUMERIC_MIN_DISPLAY_SCALE);
    rscale = Min(rscale, NUMERIC_MAX_DISPLAY_SCALE);

    // Perform square root calculation
    sqrt_var(&arg, &result, rscale);

    // Create and return result
    res = make_result(&result);
    free_var(&result);

    PG_RETURN_NUMERIC(res);
}
```
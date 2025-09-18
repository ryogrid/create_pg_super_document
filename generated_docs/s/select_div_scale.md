# select_div_scale

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:987-1052](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L987-L1052)

## Overview
Determines the appropriate result scale (number of digits after decimal point) for division operations in PostgreSQL's numeric data type.

## Definition
```c
static int select_div_scale(const NumericVar *var1, const NumericVar *var2)
```

## Detailed Description
This function implements PostgreSQL's default scale selection algorithm for division operations. It calculates a result scale that ensures at least NUMERIC_MIN_SIG_DIGITS significant digits in the quotient, providing accuracy no less than float8 precision. The function analyzes the weights and first significant digits of both operands to estimate the quotient weight, then determines an appropriate scale. The algorithm ensures the result scale is not less than either input's display scale and falls within the allowed range of PostgreSQL numeric display scales.

## Parameters / Member Variables
- `var1`: Pointer to the dividend (numerator) NumericVar
- `var2`: Pointer to the divisor (denominator) NumericVar

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (digit type)
  - NUMERIC_MIN_SIG_DIGITS (minimum significant digits constant)
  - NUMERIC_MIN_DISPLAY_SCALE (minimum display scale)
  - NUMERIC_MAX_DISPLAY_SCALE (maximum display scale)
  - DEC_DIGITS (digits per numeric base unit)
- Called from (representative examples):
  - [numeric_div_opt_error](../n/numeric_div_opt_error.md) (main division function)
  - [compute_bucket](../c/compute_bucket.md) (statistical functions)
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md) (standard deviation calculation)

## Notes and Other Information
- Returns the calculated scale as an integer
- Used internally by PostgreSQL's numeric division operations
- Ensures division results maintain appropriate precision
- The scale selection follows SQL standard guidelines for maintaining precision
- Critical for maintaining numeric precision in financial and scientific calculations
- Balances precision requirements with performance considerations
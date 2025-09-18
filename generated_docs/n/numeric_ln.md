# numeric_ln

## Location
src/backend/utils/adt/numeric.c: 3831 - 3879

## Overview
Computes the natural logarithm (ln) of a numeric value with optimized scale determination and mathematical error handling.

## Definition


## Detailed Description
The  function calculates the natural logarithm of a numeric input value. It enforces mathematical constraints by raising an error for negative infinity inputs, as logarithms are undefined for negative numbers. The function preserves NaN and positive infinity values as per mathematical conventions.

A key optimization is the use of  to predict the decimal weight of the logarithm result before performing the actual calculation. This allows for intelligent scale selection that ensures adequate precision while avoiding unnecessary computational overhead. The function maintains at least  significant digits in the result.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing the input numeric value

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_NUMERIC: Extract numeric argument from function args
  - NUMERIC_IS_SPECIAL: Check if numeric is NaN or infinity
  - NUMERIC_IS_NINF: Check if numeric is negative infinity
  - [duplicate_numeric](../d/duplicate_numeric.md): Create copy of numeric value for NaN/+∞
  - [init_var_from_num](../i/init_var_from_num.md): Initialize NumericVar from Numeric
  - init_var: Initialize empty NumericVar
  - [estimate_ln_dweight](../e/estimate_ln_dweight.md): Estimate decimal weight of logarithm result
  - [ln_var](../l/ln_var.md): Core natural logarithm calculation function
  - [make_result](../m/make_result.md): Convert NumericVar to Numeric result
  - [free_var](../f/free_var.md): Free NumericVar memory
  - PG_RETURN_NUMERIC: Return numeric result
- Called from (representative examples):
  - SQL ln() function calls
  - Logarithmic numeric expressions

## Notes and Other Information
- Raises  error for negative infinity inputs
- Uses  for efficient scale pre-calculation
- [Result](../R/Result.md) scale bounded by  and 
- Delegates actual logarithm computation to  function
- Preserves input scale as minimum result scale requirement
- Located in 
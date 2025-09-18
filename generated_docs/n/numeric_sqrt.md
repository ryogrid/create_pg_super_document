# numeric_sqrt

## Location
src/backend/utils/adt/numeric.c: 3692 - 3763

## Overview
Computes the square root of a numeric value with appropriate scale handling and special value processing.

## Definition


## Detailed Description
The  function calculates the square root of a PostgreSQL numeric data type. It handles special numeric values (NaN, positive and negative infinity) according to mathematical conventions. For negative infinity, it raises an error since square roots of negative numbers are undefined in real arithmetic. For NaN and positive infinity, it returns the same special value.

The function carefully determines the appropriate result scale to ensure at least  significant digits while respecting the input's decimal scale. It uses an optimized weight calculation that accounts for whether  is even or odd to minimize computational overhead.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing the input numeric value

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
  - Numeric operator expressions

## Notes and Other Information
- Raises  error for negative infinity inputs
- Scale calculation optimizes for even  values to avoid rounding operations
- [Result](../R/Result.md) scale is bounded by  and 
- Uses  for the actual mathematical computation
- Located in 
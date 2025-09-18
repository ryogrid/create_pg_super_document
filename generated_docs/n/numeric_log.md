# numeric_log

## Location
src/backend/utils/adt/numeric.c: 3880 - 3950

## Overview
Computes the logarithm of one numeric value using another numeric value as the base, with comprehensive special value handling and mathematical constraint validation.

## Definition


## Detailed Description
The  function calculates log_base2(value1) where the first argument is the value and the second argument is the base. It implements comprehensive special value semantics: log(∞, ∞) returns NaN due to the indeterminate form ∞/∞, log(∞, finite) returns 0, and log(finite, ∞) returns ∞. The function enforces mathematical constraints by rejecting negative inputs and zero inputs with appropriate error messages.

The implementation handles all combinations of special numeric values (NaN, ±∞) according to mathematical conventions. For finite inputs, it delegates the actual logarithm computation to , which handles scale selection internally. This design separates special value handling from the core mathematical computation.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing two numeric values (value and base)

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
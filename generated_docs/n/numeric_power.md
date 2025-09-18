# numeric_power

## Location
src/backend/utils/adt/numeric.c: 3951 - 4137

## Overview
Computes x raised to the power of y (x^y) with extensive special value handling following POSIX pow(3) specifications and SQL standards.

## Definition


## Detailed Description
The  function implements exponentiation for PostgreSQL numeric types with comprehensive handling of special values including NaN and infinities. It strictly follows POSIX pow(3) semantics: NaN^0 = 1, 1^NaN = 1, while other NaN combinations return NaN. The function implements detailed rules for infinity combinations, such as |x|<1 and y=±∞, |x|>1 and y=±∞, and special cases like (-1)^∞ = 1.

Mathematical constraints are enforced by raising appropriate errors for undefined operations like 0^(negative) and negative^(non-integer). The function validates that negative bases are only raised to integral powers to avoid complex results. For finite inputs, computation is delegated to  which handles scale selection internally.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing two numeric values (base and exponent)

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
# numericvar_to_int128

## Location
src/backend/utils/adt/numeric.c: 8240 - 8310

## Overview
Converts a PostgreSQL NumericVar to a 128-bit signed integer, performing rounding if necessary and detecting overflow conditions.

## Definition


## Detailed Description
This function converts a PostgreSQL numeric value represented as a NumericVar structure to a 128-bit signed integer (int128). The conversion process includes automatic rounding to the nearest integer and comprehensive overflow detection. The function is designed to handle the full range of int128 values, including the special case of INT128_MIN which requires careful overflow checking due to two's complement arithmetic properties.

The conversion algorithm first rounds the input to the nearest integer, then constructs the result by processing the numeric digits from most significant to least significant, multiplying by the numeric base (NBASE) at each step. Overflow detection is performed at each multiplication step to ensure the result fits within the int128 range.

## Parameters / Member Variables
- : Pointer to the input NumericVar structure containing the numeric value to convert
- : Pointer to int128 where the converted result will be stored

## Dependencies
- Functions called/Symbols referenced:
  - init_var: Initialize a NumericVar structure
  - [set_var_from_var](../s/set_var_from_var.md): Copy one NumericVar to another
  - [round_var](../r/round_var.md): Round a NumericVar to specified decimal places
  - [strip_var](../s/strip_var.md): Remove leading/trailing zeros from NumericVar
  - [free_var](../f/free_var.md): Free memory allocated for NumericVar
  - NUMERIC_NEG: Constant indicating negative sign
  - NBASE: Numeric base constant (10000)
  - NumericDigit: Type for numeric digit storage

- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT: Numeric optimization function
  - [numeric_poly_deserialize](numeric_poly_deserialize.md): Deserialization function for numeric polynomials
  - [int8_avg_deserialize](../i/int8_avg_deserialize.md): Deserialization function for int8 averages

## Notes and Other Information
- Returns true on successful conversion, false on overflow
- Handles the special case of INT128_MIN overflow detection using the property that -val == val only for INT128_MIN in two's complement arithmetic
- Input is automatically rounded to the nearest integer before conversion
- The function assumes weight >= 0 and ndigits <= weight + 1 after stripping
- Memory management is handled properly with free_var() calls on all exit paths
- Zero input is handled as a special case for efficiency
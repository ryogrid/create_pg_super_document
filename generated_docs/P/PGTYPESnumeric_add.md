# PGTYPESnumeric_add

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 637 - 764

## Overview
The PGTYPESnumeric_add function performs addition of two numeric values with full sign handling, serving as the high-level interface for numeric addition in PostgreSQL's ECPG pgtypes library.

## Definition
int PGTYPESnumeric_add(numeric *var1, numeric *var2, numeric *result)

## Detailed Description
The PGTYPESnumeric_add function implements signed numeric addition by analyzing the signs of both operands and delegating to appropriate low-level functions (add_abs and sub_abs) based on the sign combinations. The function handles all four possible sign combinations: both positive, both negative, mixed signs with different magnitude relationships. When operands have different signs, the function compares absolute values to determine which operation to perform and what sign the result should have. The function ensures proper scaling and handles special cases like equal absolute values that result in zero.

## Parameters / Member Variables
- `var1`: Pointer to the first numeric operand
- `var2`: Pointer to the second numeric operand  
- `result`: Pointer to the numeric structure where the sum will be stored

## Dependencies
- Functions called/Symbols referenced:
  - add_abs
  - sub_abs
  - cmp_abs
  - zero_var
  - NUMERIC_POS (constant)
  - NUMERIC_NEG (constant)
  - numeric (type)
- Called from (representative examples):
  - decadd (in informix compatibility library)
  - main (in test programs)
  - decimal (in test programs)

## Notes and Other Information
- Returns 0 on success, -1 on error
- Handles all four sign combinations: (+,+), (+,-), (-,+), (-,-)
- For same signs: performs addition of absolute values with appropriate result sign
- For different signs: compares absolute values and performs subtraction with appropriate result sign
- When absolute values are equal with different signs, result is zero with proper scaling
- The result parameter can safely point to one of the operands without causing issues
- Part of the ECPG pgtypes library providing PostgreSQL-compatible numeric operations for client applications
- Result scaling (rscale and dscale) is handled appropriately for all operation types
- The function is designed to be a public interface, unlike the internal static functions it calls
# PGTYPESnumeric_sub

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 765 - 895

## Overview
The PGTYPESnumeric_sub function performs subtraction of two numeric values with full sign handling, serving as the high-level interface for numeric subtraction in PostgreSQL's ECPG pgtypes library.

## Definition
int PGTYPESnumeric_sub(numeric *var1, numeric *var2, numeric *result)

## Detailed Description
The PGTYPESnumeric_sub function implements signed numeric subtraction by analyzing the signs of both operands and delegating to appropriate low-level functions (add_abs and sub_abs) based on the sign combinations. The subtraction operation var1 - var2 is transformed into appropriate addition or subtraction of absolute values depending on the operand signs. When operands have the same sign, the function compares absolute values to determine operation order and result sign. When operands have different signs, subtraction becomes addition of absolute values with appropriate sign determination. The function ensures proper scaling and handles special cases like equal absolute values that result in zero.

## Parameters / Member Variables
- `var1`: Pointer to the first numeric operand (minuend)
- `var2`: Pointer to the second numeric operand (subtrahend)
- `result`: Pointer to the numeric structure where the difference will be stored

## Dependencies
- Functions called/Symbols referenced:
  - [add_abs](../a/add_abs.md)
  - [sub_abs](../s/sub_abs.md)
  - [cmp_abs](../c/cmp_abs.md)
  - [zero_var](../z/zero_var.md)
  - NUMERIC_POS (constant)
  - NUMERIC_NEG (constant)
  - [numeric](../n/numeric.md) (type)
- Called from (representative examples):
  - [decsub](../d/decsub.md) (in informix compatibility library)
  - [main](../m/main.md) (in test programs)
  - decimal (in test programs)

## Notes and Other Information
- Returns 0 on success, -1 on error
- Handles all four sign combinations for subtraction: (+,+), (+,-), (-,+), (-,-)
- For (+,-): becomes addition with positive result: +(|var1| + |var2|)
- For (-,+): becomes addition with negative result: -(|var1| + |var2|)
- For same signs: compares absolute values and performs subtraction with appropriate result sign
- When absolute values are equal with same signs, result is zero with proper scaling
- The result parameter can safely point to one of the operands without causing issues
- Part of the ECPG pgtypes library providing PostgreSQL-compatible numeric operations for client applications
- [Result](../R/Result.md) scaling (rscale and dscale) is handled appropriately for all operation types
- The function is designed to be a public interface, unlike the internal static functions it calls
- Subtraction with different signs effectively becomes addition, while subtraction with same signs requires magnitude comparison
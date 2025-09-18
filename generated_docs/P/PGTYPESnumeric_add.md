# PGTYPESnumeric_add

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:637-764](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L637-L764)

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
  - [add_abs](../a/add_abs.md)
  - [sub_abs](../s/sub_abs.md)
  - [cmp_abs](../c/cmp_abs.md)
  - [zero_var](../z/zero_var.md)
  - NUMERIC_POS (constant)
  - NUMERIC_NEG (constant)
  - [numeric](../n/numeric.md) (type)
- Called from (representative examples):
  - [decadd](../d/decadd.md) (in informix compatibility library)
  - [main](../m/main.md) (in test programs)
  - decimal (in test programs)

## Notes and Other Information
- Returns 0 on success, -1 on error
- Handles all four sign combinations: (+,+), (+,-), (-,+), (-,-)
- For same signs: performs addition of absolute values with appropriate result sign
- For different signs: compares absolute values and performs subtraction with appropriate result sign
- When absolute values are equal with different signs, result is zero with proper scaling
- The result parameter can safely point to one of the operands without causing issues
- Part of the ECPG pgtypes library providing PostgreSQL-compatible numeric operations for client applications
- [Result](../R/Result.md) scaling (rscale and dscale) is handled appropriately for all operation types
- The function is designed to be a public interface, unlike the internal static functions it calls
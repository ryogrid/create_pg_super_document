# zero_var

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 374 - 384

## Overview
Sets a PostgreSQL NumericVar variable to represent the value zero while preserving its decimal scale setting.

## Definition
```c
static void zero_var(NumericVar *var)
```

## Detailed Description
This internal function initializes or resets a NumericVar structure to represent the numeric value zero. It frees any existing digit buffer memory and sets all the structural fields to their zero-value equivalents. Importantly, the function does not modify the dscale (decimal scale) field, allowing the precision setting to be preserved across zero operations. This is a fundamental utility function used throughout PostgreSQL's numeric arithmetic operations.

## Parameters / Member Variables
- `var`: Pointer to the NumericVar structure to be set to zero

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_free (for releasing digit buffer memory)
  - NUMERIC_POS (constant for positive sign designation)
- Called from (representative examples):
  - [add_var](../a/add_var.md), sub_var, mul_var, div_var (arithmetic operations)
  - [sqrt_var](../s/sqrt_var.md), exp_var, power_var (mathematical functions)
  - [PGTYPESnumeric_add](../P/PGTYPESnumeric_add.md), PGTYPESnumeric_sub, PGTYPESnumeric_div (ECPG functions)
  - [set_var_from_non_decimal_integer_str](../s/set_var_from_non_decimal_integer_str.md) (string parsing)

## Notes and Other Information
- This is a static function internal to the numeric.c module
- Preserves the dscale field intentionally for precision consistency
- Sets sign to NUMERIC_POS as a convention (could be any non-NAN value)
- Weight is set to 0 by convention, though it doesn't affect zero representation
- Essential building block for numeric arithmetic and initialization operations
- Located in src/backend/utils/adt/numeric.c:7001-7027
# PGTYPESnumeric_div

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 1053 - 1280

## Overview
Performs division of two numeric values in PostgreSQL's ECPG pgtypes library using long division algorithm with variable precision.

## Definition
```c
int PGTYPESnumeric_div(numeric *var1, numeric *var2, numeric *result)
```

## Detailed Description
This function implements division of two numeric variables using a sophisticated long division algorithm. It handles division by zero checks, determines result sign and precision, and uses an iterative approach with digit-by-digit calculation. The function maintains an array of pre-computed multiples of the divisor (divisor[1] through divisor[9]) to optimize the division process. It employs a guessing strategy to determine each quotient digit, then performs subtraction and continues until the desired precision is achieved. The algorithm includes proper rounding, leading/trailing zero handling, and memory management.

## Parameters / Member Variables
- `var1`: Pointer to the dividend (numerator) numeric variable
- `var2`: Pointer to the divisor (denominator) numeric variable  
- `result`: Pointer to the numeric variable to store the division result

## Dependencies
- Functions called/Symbols referenced:
  - [select_div_scale](../s/select_div_scale.md) (determines appropriate result scale)
  - digitbuf_alloc/digitbuf_free (memory management)
  - [zero_var](../z/zero_var.md) (initializes result to zero)
  - init_var (initializes temporary variables)
  - [cmp_abs](../c/cmp_abs.md) (compares absolute values)
  - [sub_abs](../s/sub_abs.md) (performs absolute subtraction)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - PGTYPES_NUM_DIVIDE_ZERO (division by zero error)
- Called from (representative examples):
  - [decdiv](../d/decdiv.md) (in ECPG Informix compatibility layer)
  - [main](../m/main.md) (in various pgtypes test programs)

## Notes and Other Information
- Returns 0 on success, -1 on failure (memory allocation or division by zero)
- Implements proper division by zero detection and error handling
- Uses pre-computed divisor multiples to improve performance
- Handles proper rounding based on the remainder
- Manages complex memory allocation for intermediate calculations
- Part of the ECPG pgtypes library for client-side numeric operations
- Critical for financial and scientific calculations requiring exact decimal arithmetic
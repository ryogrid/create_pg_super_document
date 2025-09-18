# PGTYPESnumeric_mul

## Location
src/interfaces/ecpg/pgtypeslib/numeric.c: 896 - 986

## Overview
Performs multiplication of two numeric values in PostgreSQL's ECPG pgtypes library, implementing variable-precision decimal arithmetic.

## Definition
```c
int PGTYPESnumeric_mul(numeric *var1, numeric *var2, numeric *result)
```

## Detailed Description
This function multiplies two numeric variables using a digit-by-digit multiplication algorithm similar to traditional long multiplication. It handles variable-precision decimal arithmetic with proper sign determination, digit buffer management, and precision scaling. The function allocates a result buffer, performs the multiplication using nested loops to compute partial products, handles carry propagation, and manages leading/trailing zero removal. The result accuracy is determined by combining the scales of both input operands.

## Parameters / Member Variables
- `var1`: Pointer to the first numeric operand (multiplicand)
- `var2`: Pointer to the second numeric operand (multiplier)  
- `result`: Pointer to the numeric variable to store the multiplication result

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_alloc (for memory allocation)
  - digitbuf_free (for memory deallocation)
  - NUMERIC_POS/NUMERIC_NEG (sign constants)
  - NumericDigit (digit type)
- Called from (representative examples):
  - decmul (in ECPG Informix compatibility layer)
  - main (in various pgtypes test programs)

## Notes and Other Information
- Returns 0 on success, -1 on failure (memory allocation error)
- The result scale is the sum of both operand scales
- Uses base-10 digit arithmetic with carry propagation
- Handles sign determination: same signs result in positive, different signs result in negative
- Manages dynamic memory allocation for the result digit buffer
- Part of the ECPG (Embedded SQL in C) pgtypes library for client-side numeric operations
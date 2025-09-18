# add_abs

## Location
src/backend/utils/adt/numeric.c: 11600 - 11684

## Overview
Performs addition of the absolute values of two NumericVar operands, storing the result in a destination NumericVar with proper carry handling and memory management.

## Definition


## Detailed Description
This function implements the core algorithm for adding absolute values of two numeric variables in PostgreSQL's internal NumericVar format. It performs unsigned addition at the lowest level, handling digit-by-digit addition with carry propagation across NBASE digits.

The algorithm works by:
1. Determining the result's dimensions (weight, scale, number of digits) based on the larger of the two operands
2. Allocating a result buffer with space for potential carry overflow
3. Performing right-to-left digit addition with carry propagation using base-NBASE arithmetic
4. Properly aligning digits based on their weights (decimal positions)
5. Cleaning up the result by removing leading/trailing zeros

The function is designed to be safe for in-place operations where the result pointer may be the same as one of the input operands. It handles arbitrary precision arithmetic and maintains exact results without rounding.

## Parameters / Member Variables
- : Pointer to the first NumericVar operand (treated as absolute value)
- : Pointer to the second NumericVar operand (treated as absolute value)  
- : Pointer to NumericVar where the sum of absolute values will be stored

## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_alloc (allocates memory for result digits)
  - digitbuf_free (frees old result buffer)
  - strip_var (removes leading/trailing zeros)
  - NumericDigit (type for individual digit storage)
  - NBASE (base for digit arithmetic)
- Called from (representative examples):
  - add_var (signed addition function)
  - sub_var (subtraction when operands have different signs)
  - PGTYPESnumeric_add (ECPG interface addition)
  - PGTYPESnumeric_sub (ECPG interface subtraction)

## Notes and Other Information
- Static function internal to numeric.c, part of the lowest-level arithmetic operations
- Handles arbitrary precision addition without loss of precision
- Safe for in-place operations (result can point to var1 or var2)
- Uses base-NBASE arithmetic where each 'digit' represents multiple decimal digits
- Allocates one extra digit to handle potential carry overflow
- The Assert(carry == 0) ensures the buffer sizing was calculated correctly
- Result weight is set to Max(var1->weight, var2->weight) + 1 to handle potential overflow
- Decimal scale (dscale) is set to the maximum of the input scales to preserve precision
- Essential building block for all numeric addition and subtraction operations in PostgreSQL
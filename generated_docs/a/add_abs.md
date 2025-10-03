# add_abs

## Location
[src/backend/utils/adt/numeric.c:11600-11684](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11600-L11684)

## Overview
Performs addition of the absolute values of two NumericVar operands, storing the result in a destination NumericVar with proper carry handling and memory management.

## Definition

```c
static void
add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
```
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
- `*var1`: Pointer to the first NumericVar operand (treated as absolute value)
- `*var2`: Pointer to the second NumericVar operand (treated as absolute value)
- `*result`: Pointer to NumericVar where the sum of absolute values will be stored
## Dependencies
- Functions called/Symbols referenced:
  - digitbuf_alloc (allocates memory for result digits)
  - digitbuf_free (frees old result buffer)
  - [strip_var](../s/strip_var.md) (removes leading/trailing zeros)
  - NumericDigit (type for individual digit storage)
  - NBASE (base for digit arithmetic)
- Called from (representative examples):
  - [add_var](add_var.md) (signed addition function)
  - [sub_var](../s/sub_var.md) (subtraction when operands have different signs)
  - [PGTYPESnumeric_add](../P/PGTYPESnumeric_add.md) (ECPG interface addition)
  - [PGTYPESnumeric_sub](../P/PGTYPESnumeric_sub.md) (ECPG interface subtraction)

## Notes and Other Information
- Static function internal to numeric.c, part of the lowest-level arithmetic operations
- Handles arbitrary precision addition without loss of precision
- Safe for in-place operations (result can point to var1 or var2)
- Uses base-NBASE arithmetic where each 'digit' represents multiple decimal digits
- Allocates one extra digit to handle potential carry overflow
- The Assert(carry == 0) ensures the buffer sizing was calculated correctly
- [Result](../R/Result.md) weight is set to Max(var1->weight, var2->weight) + 1 to handle potential overflow
- Decimal scale (dscale) is set to the maximum of the input scales to preserve precision
- Essential building block for all numeric addition and subtraction operations in PostgreSQL

## Simplified Source

```c
static void
add_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
    NumericDigit *res_buf, *res_digits;
    int res_ndigits, res_weight, res_dscale;
    int carry = 0;

    // Copy input values for faster loop access
    int var1ndigits = var1->ndigits;
    int var2ndigits = var2->ndigits;
    NumericDigit *var1digits = var1->digits;
    NumericDigit *var2digits = var2->digits;

    // Calculate result dimensions (allow for potential carry)
    res_weight = Max(var1->weight, var2->weight) + 1;
    res_dscale = Max(var1->dscale, var2->dscale);

    int rscale1 = var1->ndigits - var1->weight - 1;
    int rscale2 = var2->ndigits - var2->weight - 1;
    int res_rscale = Max(rscale1, rscale2);

    res_ndigits = res_rscale + res_weight + 1;
    if (res_ndigits <= 0)
        res_ndigits = 1;

    // Allocate result buffer
    res_buf = digitbuf_alloc(res_ndigits + 1);
    res_digits = res_buf + 1;

    // Perform digit-by-digit addition with carry
    int i1 = res_rscale + var1->weight + 1;
    int i2 = res_rscale + var2->weight + 1;

    for (int i = res_ndigits - 1; i >= 0; i--) {
        i1--; i2--;

        // Add digits from both operands
        if (i1 >= 0 && i1 < var1ndigits)
            carry += var1digits[i1];
        if (i2 >= 0 && i2 < var2ndigits)
            carry += var2digits[i2];

        // Handle carry to next digit
        if (carry >= NBASE) {
            res_digits[i] = carry - NBASE;
            carry = 1;
        } else {
            res_digits[i] = carry;
            carry = 0;
        }
    }

    // Store result
    digitbuf_free(result->buf);
    result->ndigits = res_ndigits;
    result->buf = res_buf;
    result->digits = res_digits;
    result->weight = res_weight;
    result->dscale = res_dscale;

    // Clean up result
    strip_var(result);
}
```
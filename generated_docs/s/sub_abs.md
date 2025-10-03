# sub_abs

## Location
[src/backend/utils/adt/numeric.c:11685-11766](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11685-L11766)

## Overview
Subtracts the absolute value of var2 from the absolute value of var1 and stores the result in the result variable, used as a core arithmetic operation in PostgreSQL's numeric type implementation.

## Definition

```c
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
```
## Detailed Description
The  function performs subtraction of absolute values between two NumericVar operands. It implements multi-precision arithmetic by working with digit arrays in base NBASE representation. The function requires that the absolute value of var1 must be greater than or equal to the absolute value of var2 to ensure the result is non-negative. The operation handles borrowing between digits and properly manages the weight and scale of the result. The result can safely point to one of the operands without causing memory corruption.

## Parameters / Member Variables
- `*var1`: Pointer to the first NumericVar operand (minuend) - must have absolute value >= |var2|
- `*var2`: Pointer to the second NumericVar operand (subtrahend) to be subtracted
- `*result`: Pointer to NumericVar structure where the result ABS(var1) - ABS(var2) will be stored
## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (type for individual digits)
  - digitbuf_alloc (allocates digit buffer)
  - digitbuf_free (frees digit buffer)
  - [strip_var](strip_var.md) (removes leading/trailing zeros)
  - NBASE (numeric base constant)
- Called from (representative examples):
  - [add_var](../a/add_var.md) (addition operations)
  - [sub_var](sub_var.md) (subtraction operations)
  - [PGTYPESnumeric_add](../P/PGTYPESnumeric_add.md) (ECPG numeric addition)
  - [PGTYPESnumeric_sub](../P/PGTYPESnumeric_sub.md) (ECPG numeric subtraction)
  - [PGTYPESnumeric_div](../P/PGTYPESnumeric_div.md) (ECPG numeric division)

## Notes and Other Information
- Critical precondition: ABS(var1) MUST BE GREATER OR EQUAL ABS(var2) - violation will cause assertion failure
- The function works with digit-by-digit subtraction using borrowing mechanism
- Manages decimal scale (dscale) and weight properly for accurate decimal arithmetic
- Uses local variable copies for performance optimization in the inner loop
- Automatically strips leading and trailing zeros from the result
- Memory-safe: result parameter can alias with either input operand

## Simplified Source

```c
static void
sub_abs(const NumericVar *var1, const NumericVar *var2, NumericVar *result)
{
    NumericDigit *res_buf, *res_digits;
    int res_ndigits, res_weight, res_dscale;
    int borrow = 0;

    // Copy input values for faster loop access
    int var1ndigits = var1->ndigits;
    int var2ndigits = var2->ndigits;
    NumericDigit *var1digits = var1->digits;
    NumericDigit *var2digits = var2->digits;

    // Calculate result dimensions
    res_weight = var1->weight;
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

    // Perform digit-by-digit subtraction with borrowing
    int i1 = res_rscale + var1->weight + 1;
    int i2 = res_rscale + var2->weight + 1;

    for (int i = res_ndigits - 1; i >= 0; i--) {
        i1--; i2--;

        // Add digit from var1
        if (i1 >= 0 && i1 < var1ndigits)
            borrow += var1digits[i1];

        // Subtract digit from var2
        if (i2 >= 0 && i2 < var2ndigits)
            borrow -= var2digits[i2];

        // Handle borrowing
        if (borrow < 0) {
            res_digits[i] = borrow + NBASE;
            borrow = -1;
        } else {
            res_digits[i] = borrow;
            borrow = 0;
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
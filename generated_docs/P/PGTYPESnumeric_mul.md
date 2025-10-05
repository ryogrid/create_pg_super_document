# PGTYPESnumeric_mul

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:896-986](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L896-L986)

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
  - [decmul](../d/decmul.md) (in ECPG Informix compatibility layer)
  - [main](../m/main.md) (in various pgtypes test programs)

## Notes and Other Information
- Returns 0 on success, -1 on failure (memory allocation error)
- The result scale is the sum of both operand scales
- Uses base-10 digit arithmetic with carry propagation
- Handles sign determination: same signs result in positive, different signs result in negative
- Manages dynamic memory allocation for the result digit buffer
- Part of the ECPG (Embedded SQL in C) pgtypes library for client-side numeric operations

## Simplified Source
```c
int
PGTYPESnumeric_mul(numeric *var1, numeric *var2, numeric *result)
{
    NumericDigit *res_buf, *res_digits;
    int res_ndigits, res_weight, res_sign;
    int i, ri, i1, i2;
    long sum = 0;
    int global_rscale = var1->rscale + var2->rscale;

    // Calculate result dimensions and sign
    res_weight = var1->weight + var2->weight + 2;
    res_ndigits = var1->ndigits + var2->ndigits + 1;
    res_sign = (var1->sign == var2->sign) ? NUMERIC_POS : NUMERIC_NEG;

    // Allocate result buffer
    if ((res_buf = digitbuf_alloc(res_ndigits)) == NULL)
        return -1;
    res_digits = res_buf;
    memset(res_digits, 0, res_ndigits);

    // Perform digit-by-digit multiplication
    ri = res_ndigits;
    for (i1 = var1->ndigits - 1; i1 >= 0; i1--)
    {
        sum = 0;
        i = --ri;
        for (i2 = var2->ndigits - 1; i2 >= 0; i2--)
        {
            sum += res_digits[i] + var1->digits[i1] * var2->digits[i2];
            res_digits[i--] = sum % 10;
            sum /= 10;
        }
        res_digits[i] = sum;
    }

    // Handle rounding if needed
    i = res_weight + global_rscale + 2;
    if (i >= 0 && i < res_ndigits)
    {
        // Round and propagate carry
        sum = (res_digits[i] > 4) ? 1 : 0;
        res_ndigits = i;
        i--;
        while (sum)
        {
            sum += res_digits[i];
            res_digits[i--] = sum % 10;
            sum /= 10;
        }
    }

    // Remove leading and trailing zeros
    while (res_ndigits > 0 && *res_digits == 0)
    {
        res_digits++;
        res_weight--;
        res_ndigits--;
    }
    while (res_ndigits > 0 && res_digits[res_ndigits - 1] == 0)
        res_ndigits--;

    // Handle zero result
    if (res_ndigits == 0)
    {
        res_sign = NUMERIC_POS;
        res_weight = 0;
    }

    // Store result
    digitbuf_free(result->buf);
    result->buf = res_buf;
    result->digits = res_digits;
    result->ndigits = res_ndigits;
    result->weight = res_weight;
    result->rscale = global_rscale;
    result->sign = res_sign;
    result->dscale = var1->dscale + var2->dscale;

    return 0;
}
```
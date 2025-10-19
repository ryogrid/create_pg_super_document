# PGTYPESnumeric_div

## Location
[src/interfaces/ecpg/pgtypeslib/numeric.c:1053-1280](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/interfaces/ecpg/pgtypeslib/numeric.c#L1053-L1280)

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

## Simplified Source

```c
int PGTYPESnumeric_div(numeric *var1, numeric *var2, numeric *result) {
    // Division by zero check
    if (var2->ndigits + 1 == 1) {
        errno = PGTYPES_NUM_DIVIDE_ZERO;
        return -1;
    }

    // Determine result sign and dimensions
    int res_sign = (var1->sign == var2->sign) ? NUMERIC_POS : NUMERIC_NEG;
    int rscale = select_div_scale(var1, var2, &rscale);
    int res_weight = var1->weight - var2->weight + 1;
    int res_ndigits = rscale + res_weight;
    if (res_ndigits <= 0) res_ndigits = 1;

    // Handle zero dividend
    if (var1->ndigits == 0) {
        zero_var(result);
        result->rscale = rscale;
        return 0;
    }

    // Setup working variables
    numeric dividend, divisor[10];
    init_var(&dividend);
    for (int i = 1; i < 10; i++) init_var(&divisor[i]);

    // Copy divisor with leading zero
    divisor[1].ndigits = var2->ndigits + 1;
    divisor[1].digits[0] = 0;
    memcpy(&divisor[1].digits[1], var2->digits, var2->ndigits);

    // Copy dividend
    dividend.ndigits = var1->ndigits;
    memcpy(dividend.digits, var1->digits, var1->ndigits);

    // Allocate result buffer
    result->buf = digitbuf_alloc(res_ndigits + 2);
    result->ndigits = res_ndigits;
    result->weight = res_weight;
    result->sign = res_sign;

    // Long division algorithm
    long first_div = divisor[1].digits[1] * 10 + divisor[1].digits[2];
    long first_have = 0;
    int first_nextdigit = 0;

    for (int ri = 0; ri <= res_ndigits; ri++) {
        // Get next dividend digits
        first_have = first_have * 10;
        if (first_nextdigit < dividend.ndigits)
            first_have += dividend.digits[first_nextdigit];
        first_nextdigit++;

        // Estimate quotient digit
        long guess = (first_have * 10) / first_div + 1;
        if (guess > 9) guess = 9;

        // Find largest valid quotient digit
        while (guess > 0) {
            // Create divisor multiple if needed
            if (divisor[guess].buf == NULL) {
                // Multiply divisor[1] by guess
                multiply_divisor(&divisor[1], guess, &divisor[guess]);
            }

            // Check if dividend >= divisor[guess]
            if (cmp_abs(&dividend, &divisor[guess]) >= 0)
                break;
            guess--;
        }

        // Store quotient digit
        result->digits[ri + 1] = guess;

        // Subtract divisor multiple from dividend
        if (guess > 0) {
            sub_abs(&dividend, &divisor[guess], &dividend);
        }
    }

    // Handle rounding and cleanup leading/trailing zeros
    cleanup_result(result);

    // Free temporary buffers
    cleanup_vars(&dividend, divisor);

    return 0;  // Success
}
```
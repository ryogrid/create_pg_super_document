# round_var

## Location
[src/backend/utils/adt/numeric.c:11767-11872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11767-L11872)

## Overview
Rounds a NumericVar value to a specified number of decimal digits after the decimal point, supporting both positive and negative rscale values for comprehensive numeric precision control.

## Definition

```c
static void
round_var(NumericVar *var, int rscale)
```
## Detailed Description
The  function implements decimal rounding for PostgreSQL's numeric type by truncating or extending precision to a specified number of decimal places. It supports negative rscale values, allowing rounding before the decimal point (e.g., rounding to nearest 10, 100, etc.). The function uses banker's rounding (round half to even) and handles carry propagation when rounding causes overflow. It works with the internal NBASE digit representation and properly manages the weight and scale attributes of the NumericVar structure.

## Parameters / Member Variables
- : Pointer to NumericVar structure to be rounded (modified in place)
- : Target number of decimal digits after decimal point (can be negative for rounding before decimal point)

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (type for individual digits)
  - DEC_DIGITS (decimal digits per NBASE digit constant)
  - NUMERIC_POS (positive sign constant)
  - HALF_NBASE (half of numeric base for rounding comparison)
  - NBASE (numeric base constant)
  - round_powers (power-of-10 lookup table for DEC_DIGITS=4)
- Called from (representative examples):
  - [numeric_round](../n/numeric_round.md) (SQL ROUND function)
  - [numeric_mul_opt_error](../n/numeric_mul_opt_error.md) (multiplication with error checking)
  - [apply_typmod](../a/apply_typmod.md) (applying type modifiers)
  - [numericvar_to_int64](../n/numericvar_to_int64.md) (conversion to 64-bit integer)
  - [mul_var](../m/mul_var.md), div_var, sqrt_var (arithmetic operations)
  - [exp_var](../e/exp_var.md), power_var_int (mathematical functions)

## Notes and Other Information
- Supports rscale < 0 for rounding before decimal point (e.g., rscale=-2 rounds to nearest hundred)
- Uses conditional compilation for different DEC_DIGITS values (1, 2, 4)
- Implements proper carry propagation when rounding causes digit overflow
- May adjust the digits pointer and weight when carry extends beyond existing digits
- The dscale field is immediately set to the target rscale value
- For very negative rscale values that eliminate all significant digits, the result becomes 0
- Uses modular arithmetic for within-digit rounding when DEC_DIGITS > 1

## Simplified Source

```c
static void round_var(NumericVar *var, int rscale) {
    NumericDigit *digits = var->digits;
    int di, ndigits, carry;

    var->dscale = rscale;

    // Calculate decimal digits wanted
    di = (var->weight + 1) * DEC_DIGITS + rscale;

    // Handle cases where result becomes zero
    if (di < 0) {
        var->ndigits = 0;
        var->weight = 0;
        var->sign = NUMERIC_POS;
        return;
    }

    // Calculate NBASE digits needed
    ndigits = (di + DEC_DIGITS - 1) / DEC_DIGITS;
    di %= DEC_DIGITS;

    // Check if rounding is needed
    if (ndigits < var->ndigits || (ndigits == var->ndigits && di > 0)) {
        var->ndigits = ndigits;

        // Determine if we need to carry (round up)
        if (di == 0) {
            // Round between NBASE digits
            carry = (digits[ndigits] >= HALF_NBASE) ? 1 : 0;
        } else {
            // Round within NBASE digit using power of 10
            int extra, pow10 = round_powers[di];
            extra = digits[--ndigits] % pow10;
            digits[ndigits] -= extra;
            carry = 0;

            if (extra >= pow10 / 2) {
                pow10 += digits[ndigits];
                if (pow10 >= NBASE) {
                    pow10 -= NBASE;
                    carry = 1;
                }
                digits[ndigits] = pow10;
            }
        }

        // Propagate carry through digits
        while (carry) {
            carry += digits[--ndigits];
            if (carry >= NBASE) {
                digits[ndigits] = carry - NBASE;
                carry = 1;
            } else {
                digits[ndigits] = carry;
                carry = 0;
            }
        }

        // Adjust if carry extends beyond existing digits
        if (ndigits < 0) {
            var->digits--;
            var->ndigits++;
            var->weight++;
        }
    }
}
```
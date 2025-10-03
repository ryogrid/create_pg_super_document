# strip_var

## Location
[src/backend/utils/adt/numeric.c:11935-11975](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11935-L11975)

## Overview
Removes leading and trailing zero digits from a NumericVar to maintain canonical representation and optimize storage efficiency.

## Definition

```c
static void
strip_var(NumericVar *var)
```
## Detailed Description
The  function normalizes a NumericVar by removing unnecessary leading and trailing zero digits that don't contribute to the numeric value. Leading zeros are removed by advancing the digits pointer and decreasing the weight accordingly. Trailing zeros are removed by reducing the ndigits count. When all digits are removed (resulting in a zero value), the function normalizes the sign to positive and sets the weight to zero. This function is essential for maintaining the canonical form of numeric values and preventing unnecessary storage overhead.

## Parameters / Member Variables
- `*var`: Pointer to NumericVar structure to be normalized (modified in place)
## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (type for individual digits)
  - NUMERIC_POS (positive sign constant for zero normalization)
- Called from (representative examples):
  - [set_var_from_str](set_var_from_str.md) (parsing numeric strings)
  - [numericvar_to_int64](../n/numericvar_to_int64.md), numericvar_to_uint64, numericvar_to_int128 (numeric conversions)
  - [mul_var](../m/mul_var.md) (multiplication operations)
  - [div_var](../d/div_var.md), div_var_fast, div_var_int, div_var_int64 (division operations)
  - [sqrt_var](sqrt_var.md) (square root calculation)
  - [random_var](../r/random_var.md) (random number generation)
  - [add_abs](../a/add_abs.md), sub_abs (absolute value arithmetic)
  - [accum_sum_final](../a/accum_sum_final.md) (sum accumulation finalization)

## Notes and Other Information
- Critical for maintaining canonical representation of numeric values
- Adjusts both digits pointer and weight when removing leading zeros
- Essential for preventing storage bloat from unnecessary zero digits
- Automatically normalizes zero values to have positive sign and zero weight
- Called after most arithmetic operations to ensure clean results
- Does not allocate or free memory - only adjusts pointers and counts
- Preserves the original digit buffer while potentially changing the active digit range
- Part of the fundamental numeric value maintenance infrastructure in PostgreSQL

## Simplified Source

```c
static void
strip_var(NumericVar *var)
{
    NumericDigit *digits = var->digits;
    int ndigits = var->ndigits;

    // Remove leading zeros by advancing the digits pointer
    while (ndigits > 0 && *digits == 0) {
        digits++;
        var->weight--;  // Adjust weight as we skip leading digits
        ndigits--;
    }

    // Remove trailing zeros by reducing digit count
    while (ndigits > 0 && digits[ndigits - 1] == 0) {
        ndigits--;
    }

    // Special case: if all digits were zeros, normalize to canonical zero
    if (ndigits == 0) {
        var->sign = NUMERIC_POS;
        var->weight = 0;
    }

    // Update the NumericVar with cleaned values
    var->digits = digits;
    var->ndigits = ndigits;
}
```
# int64_to_numericvar

## Location
[src/backend/utils/adt/numeric.c:8120-8166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8120-L8166)

## Overview
The `int64_to_numericvar` function converts a 64-bit signed integer value into PostgreSQL's internal numeric variable representation, handling sign conversion and digit allocation efficiently.

## Definition

```c
static void
int64_to_numericvar(int64 val, NumericVar *var)
```
## Detailed Description
This function performs the conversion from a native 64-bit integer to PostgreSQL's arbitrary-precision numeric format:

1. **Memory Allocation**: Allocates sufficient space for the maximum possible digits needed (20 decimal digits for int64)
2. **Sign Handling**: Determines and sets the numeric sign, converting negative values to positive for processing
3. **Zero Optimization**: Special case handling for zero values to avoid unnecessary computation
4. **Digit Extraction**: Uses division by NBASE to extract digits in reverse order (least to most significant)
5. **Buffer Management**: Efficiently places digits in the allocated buffer using pointer arithmetic
6. **Weight Calculation**: Sets the weight field to indicate the position of the most significant digit

The algorithm extracts digits by repeated division, building the result from the least significant digit upward.

## Parameters / Member Variables
- `val`: The 64-bit signed integer value to convert
- `var`: Pointer to the target NumericVar structure to populate with the converted value

## Dependencies
- Functions called/Symbols referenced:
  - [alloc_var](../a/alloc_var.md): Allocate memory for numeric variable digits
  - `NBASE`: Numeric digit base constant (typically 10000)
  - `NUMERIC_NEG`: Constant representing negative sign
  - `NUMERIC_POS`: Constant representing positive sign  
  - `DEC_DIGITS`: Number of decimal digits per internal digit
  - `NumericDigit`: Type for individual digit storage

- Called from (representative examples):
  - `NUMERIC_CAN_BE_SHORT`: Short numeric creation path
  - `[width_bucket_numeric](../w/width_bucket_numeric.md)`: Width bucket calculation
  - [numeric_fac](../n/numeric_fac.md): Factorial computation
  - [int64_to_numeric](int64_to_numeric.md): Public int64 conversion interface
  - [int64_div_fast_to_numeric](int64_div_fast_to_numeric.md): Fast division with numeric result
  - [numeric_stddev_internal](../n/numeric_stddev_internal.md): Statistical functions
  - [set_var_from_non_decimal_integer_str](../s/set_var_from_non_decimal_integer_str.md): Non-decimal string parsing
  - [sqrt_var](../s/sqrt_var.md): Square root computation

## Notes and Other Information
- Allocates space for up to 20 decimal digits to handle the full int64 range safely
- Uses unsigned arithmetic internally to handle INT64_MIN correctly
- Extracts digits in reverse order for efficiency, then adjusts the buffer pointer
- Sets dscale to 0 since integers have no fractional part
- The weight field represents the number of digits before the decimal point minus 1
- Handles the zero case efficiently without digit extraction
- Uses pointer arithmetic to minimize memory copying
- The division-based digit extraction works with any NBASE value

## Simplified Source

```c
static void
int64_to_numericvar(int64 val, NumericVar *var)
{
    uint64 uval;
    NumericDigit *ptr;
    int ndigits;

    // Allocate space for max 19 decimal digits (plus safety margin)
    alloc_var(var, 20 / DEC_DIGITS);

    // Handle sign and convert to unsigned
    if (val < 0) {
        var->sign = NUMERIC_NEG;
        uval = -val;
    } else {
        var->sign = NUMERIC_POS;
        uval = val;
    }

    var->dscale = 0;  // No fractional part for integers

    // Special case: zero
    if (val == 0) {
        var->ndigits = 0;
        var->weight = 0;
        return;
    }

    // Extract digits by repeated division
    ptr = var->digits + var->ndigits;
    ndigits = 0;

    do {
        ptr--;
        ndigits++;
        uint64 next_val = uval / NBASE;
        *ptr = uval - next_val * NBASE;  // Store remainder as digit
        uval = next_val;
    } while (uval);

    // Set final numeric properties
    var->digits = ptr;
    var->ndigits = ndigits;
    var->weight = ndigits - 1;  // Position of most significant digit
}
```
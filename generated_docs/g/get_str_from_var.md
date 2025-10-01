# get_str_from_var

## Location
[src/backend/utils/adt/numeric.c:7510-7662](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L7510-L7662)

## Overview
Converts a NumericVar to its text representation, handling proper formatting of digits before and after the decimal point according to the variable's display scale.

## Definition

```c
static char *
get_str_from_var(const NumericVar *var)
```
## Detailed Description
This static function serves as the core conversion routine for transforming a NumericVar into a human-readable string representation. It handles the complex task of properly formatting numeric digits into decimal notation, including sign handling, decimal point placement, and trailing zero management according to the variable's dscale (display scale).

The function allocates memory for the result string, accounting for the maximum possible length including sign, digits before decimal point, decimal point, digits after decimal point, and null terminator. It processes digits in groups according to the DEC_DIGITS configuration (typically 4 digits per NumericDigit), carefully handling leading zero suppression before the decimal point and proper truncation after it.

The algorithm walks through the digit array, converting each NumericDigit into its constituent decimal digits while respecting the weight (position of decimal point) and dscale (number of digits to display after decimal point) parameters.

## Parameters / Member Variables
- `var`: Source NumericVar to convert to string representation (const pointer for read-only access)

## Dependencies
- Functions called/Symbols referenced:
  - [palloc](../p/palloc.md) (allocates memory for result string)
  - NumericDigit (typedef for packed digit storage)
  - DEC_DIGITS (compile-time constant defining digits per NumericDigit)
  - NUMERIC_NEG (constant for negative sign value)
- Called from (representative examples):
  - [numeric_out](../n/numeric_out.md) (main numeric output function)
  - [numeric_normalize](../n/numeric_normalize.md) (normalization operations)
  - [get_str_from_var_sci](get_str_from_var_sci.md) (scientific notation formatting)
  - [numericvar_to_double_no_overflow](../n/numericvar_to_double_no_overflow.md) (conversion to double)
  - [PGTYPESnumeric_to_asc](../P/PGTYPESnumeric_to_asc.md) (ECPG library conversion)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c module
- Returns a palloc'd string that must be freed by the caller
- Handles different DEC_DIGITS configurations (1, 2, or 4 digits per NumericDigit)
- Suppresses leading zeros before the decimal point but preserves trailing zeros after decimal point according to dscale
- Properly handles edge cases like zero values and negative numbers
- The output format respects the dscale setting for controlling decimal places
- Used as the foundation for all numeric-to-string conversions in PostgreSQL
- Handles very large and very small numbers through the weight/dscale mechanism
- The algorithm is optimized for the internal numeric representation used by PostgreSQL

## Simplified Source

```c
static char *get_str_from_var(const NumericVar *var) {
    int dscale = var->dscale;
    char *str, *cp;
    int i, d;
    NumericDigit digit;

    // Allocate memory for result string (sign + digits + decimal + null terminator)
    i = (var->weight + 1) * DEC_DIGITS;
    if (i <= 0) i = 1;
    str = palloc(i + dscale + DEC_DIGITS + 2);
    cp = str;

    // Add negative sign if needed
    if (var->sign == NUMERIC_NEG)
        *cp++ = '-';

    // Output digits before decimal point
    if (var->weight < 0) {
        *cp++ = '0';  // Leading zero for numbers < 1
        d = var->weight + 1;
    } else {
        // Process each digit group, suppressing leading zeros
        for (d = 0; d <= var->weight; d++) {
            digit = (d < var->ndigits) ? var->digits[d] : 0;

            // Convert digit group to individual decimal digits
            // (Implementation varies by DEC_DIGITS: 1, 2, or 4 digits per group)
            // Suppress leading zeros in first digit group only
            format_digit_group_to_string(&cp, digit, d > 0);
        }
    }

    // Output decimal point and fractional digits if needed
    if (dscale > 0) {
        *cp++ = '.';
        char *end_pos = cp + dscale;

        // Output digit groups after decimal point
        for (i = 0; i < dscale; d++, i += DEC_DIGITS) {
            digit = (d >= 0 && d < var->ndigits) ? var->digits[d] : 0;
            format_digit_group_to_string(&cp, digit, true);
        }
        cp = end_pos;  // Truncate to exact dscale digits
    }

    *cp = '\0';
    return str;
}
```
# get_str_from_var_sci

## Location
src/backend/utils/adt/numeric.c: 7663 - 7739

## Overview
Converts a NumericVar to normalized scientific notation text representation in the form a × 10^b, displayed using E notation with proper exponent formatting.

## Definition


## Detailed Description
This static function converts a NumericVar into scientific notation format, which represents numbers as a significand multiplied by a power of 10. The function calculates the appropriate exponent to normalize the number so that there is exactly one significant digit before the decimal point, then formats the result using E notation (e.g., "1.23e+04" for 12300).

The algorithm first determines the exponent by analyzing the weight and the leading digit of the number, compensating for any leading zeros in the first numeric digit. It then divides the original number by 10^exponent to obtain the significand, rounding to the specified number of decimal places. Finally, it formats the result string combining the significand and exponent with proper E notation formatting.

Special handling is provided for zero values, which technically don't have a meaningful exponent in normalized notation but are displayed with exponent zero for consistency.

## Parameters / Member Variables
- `var`: Source NumericVar to convert to scientific notation (const pointer for read-only access)
- `rscale`: Number of decimal digits desired after the decimal point in the significand (negative values treated as zero)

## Dependencies
- Functions called/Symbols referenced:
  - DEC_DIGITS (compile-time constant defining digits per NumericDigit)
  - log10 (logarithm base 10 function for exponent calculation)
  - init_var (initializes temporary NumericVar)
  - [power_ten_int](../p/power_ten_int.md) (calculates 10^exponent)
  - [div_var](../d/div_var.md) (performs division for significand calculation)
  - [get_str_from_var](get_str_from_var.md) (converts significand to string)
  - [free_var](../f/free_var.md) (frees temporary NumericVar)
  - [palloc](../p/palloc.md) (allocates memory for result)
  - [pfree](../p/pfree.md) (frees temporary string)
  - snprintf (formats final result string)
- Called from (representative examples):
  - [numeric_out_sci](../n/numeric_out_sci.md) (main scientific notation output function)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c module
- Returns a palloc'd string that must be freed by the caller
- Uses E notation format with minimum two exponent digits (e.g., "e+03", "e-02")
- Assumes the exponent fits in an int32 range
- Handles zero values by displaying exponent as zero for consistency
- The significand is rounded to the specified rscale decimal places
- Exponent calculation compensates for leading zeros in the first numeric digit
- Output format follows printf conventions for scientific notation
- Used primarily for numeric_out_sci() function which provides scientific notation output for PostgreSQL numeric types
- The function properly handles very large and very small numbers through exponent normalization
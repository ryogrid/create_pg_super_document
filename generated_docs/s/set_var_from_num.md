# set_var_from_num

## Location
src/backend/utils/adt/numeric.c: 7436 - 7466

## Overview
Converts a packed database format Numeric value into a NumericVar variable representation for internal computation.

## Definition


## Detailed Description
This static function serves as a conversion utility that unpacks a Numeric value stored in PostgreSQL's compact database format into a NumericVar structure used for internal numeric computations. The function extracts the essential components (weight, sign, scale, and digit array) from the packed format and properly initializes the destination NumericVar with the appropriate memory allocation.

The function performs a direct memory copy of the digit array from the source Numeric to the destination NumericVar, making it an efficient conversion routine for numeric operations that require the variable-length working format.

## Parameters / Member Variables
- `num`: Source Numeric value in packed database format to be converted
- `dest`: Destination NumericVar pointer that will be initialized with the unpacked numeric data

## Dependencies
- Functions called/Symbols referenced:
  - NUMERIC_NDIGITS (macro to get number of digits)
  - [alloc_var](../a/alloc_var.md) (allocates memory for NumericVar digits)
  - NUMERIC_WEIGHT (macro to extract weight component)
  - NUMERIC_SIGN (macro to extract sign component)  
  - NUMERIC_DSCALE (macro to extract display scale)
  - NUMERIC_DIGITS (macro to get digit array pointer)
  - NumericDigit (typedef for digit storage type)
- Called from (representative examples):
  - [numeric](../n/numeric.md) (main numeric input function)
  - [numeric_round](../n/numeric_round.md) (rounding operations)
  - [numeric_trunc](../n/numeric_trunc.md) (truncation operations)
  - generate_series_step_numeric (numeric series generation)

## Notes and Other Information
- This is a static function, only accessible within the numeric.c module
- The function assumes the destination NumericVar is uninitialized and will allocate new memory
- Memory allocation is handled by alloc_var() which may fail if insufficient memory
- The digit array is copied using memcpy for efficiency
- This function is fundamental to numeric operations as it converts from storage format to computation format
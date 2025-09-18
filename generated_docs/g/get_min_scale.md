# get_min_scale

## Location
src/backend/utils/adt/numeric.c: 4152 - 4202

## Overview
A static helper function that calculates the minimum scale (number of decimal places) required to represent a numeric value without trailing zeros.

## Definition


## Detailed Description
The `get_min_scale` function determines the minimum number of decimal places needed to accurately represent a numeric value by finding the position of the last non-zero digit. It handles the internal PostgreSQL numeric representation where digits are stored in groups (NumericDigits) and accounts for trailing zeros within the last digit group. The function ensures that values like 1.2000 would return a minimum scale of 1 (for 1.2) rather than 4.

## Parameters / Member Variables
- `var`: Pointer to a NumericVar structure containing the numeric value to analyze
  - `var->ndigits`: Number of digit groups in the numeric value
  - `var->digits`: Array of NumericDigit values representing the number
  - `var->weight`: Position of the most significant digit relative to decimal point

## Dependencies
- Functions called/Symbols referenced:
  - `DEC_DIGITS` - Constant defining digits per NumericDigit group
  - `NumericDigit` - Type for individual digit groups in numeric representation
- Called from (representative examples):
  - `[numeric_min_scale](../n/numeric_min_scale.md)` - Public function that exposes this functionality
  - `[numeric_trim_scale](../n/numeric_trim_scale.md)` - Function that trims unnecessary decimal places

## Notes and Other Information
- This is a static (internal) function not exposed outside numeric.c
- Handles edge cases like zero values and negative scales gracefully
- The algorithm works backwards from the last digit group to find trailing zeros
- Uses modulo arithmetic to detect trailing zeros within individual NumericDigit groups
- Essential for implementing scale trimming and minimum scale detection operations
- Located in src/backend/utils/adt/numeric.c:4152-4202
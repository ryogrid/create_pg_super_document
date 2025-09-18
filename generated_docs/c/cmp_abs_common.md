# cmp_abs_common

## Location
src/backend/utils/adt/numeric.c: 11536 - 11599

## Overview
Core comparison function that compares the absolute values of two numeric values represented as digit arrays, implementing the low-level comparison logic for PostgreSQL's numeric type.

## Definition


## Detailed Description
This function implements the core algorithm for comparing absolute values of numeric data represented in PostgreSQL's internal format. It works directly with digit arrays and their associated metadata (number of digits and weight), making it usable by both NumericVar and Numeric types.

The comparison algorithm works in three phases:
1. **Weight comparison**: Compares digits in higher-order positions where one number has digits but the other doesn't
2. **Aligned comparison**: Compares corresponding digits when both numbers have the same weight
3. **Remaining digits**: Checks for any trailing non-zero digits that would make one number larger

The function handles PostgreSQL's base-NBASE representation where each 'digit' actually represents up to DEC_DIGITS decimal digits, and the weight indicates the position of the most significant digit.

## Parameters / Member Variables
- : Pointer to the digit array of the first number
- : Number of digits in the first number's array  
- : Weight (position of most significant digit) of the first number
- : Pointer to the digit array of the second number
- : Number of digits in the second number's array
- : Weight (position of most significant digit) of the second number

## Dependencies
- Functions called/Symbols referenced:
  - NumericDigit (type definition for individual digits)
- Called from (representative examples):
  - cmp_abs (wrapper function for NumericVar comparison)
  - cmp_var_common (signed comparison function)
  - NUMERIC_CAN_BE_SHORT (numeric optimization checks)

## Notes and Other Information
- Returns -1 if first number's absolute value is smaller, 0 if equal, 1 if larger
- Static function internal to numeric.c, serving as the main implementation for absolute value comparisons
- Designed to work with both NumericVar structures and packed Numeric format
- Efficiently handles numbers of different scales by comparing weights first
- The algorithm is optimized to short-circuit as soon as a difference is found
- Essential building block for all numeric arithmetic operations that require magnitude comparison
- Handles leading zeros correctly by skipping them during comparison
- Weight-based comparison allows efficient handling of very large or very small numbers
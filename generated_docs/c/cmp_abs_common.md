# cmp_abs_common

## Location
[src/backend/utils/adt/numeric.c:11536-11599](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L11536-L11599)

## Overview
Core comparison function that compares the absolute values of two numeric values represented as digit arrays, implementing the low-level comparison logic for PostgreSQL's numeric type.

## Definition

```c
static int
cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
			   const NumericDigit *var2digits, int var2ndigits, int var2weight)
```
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
  - [cmp_abs](cmp_abs.md) (wrapper function for NumericVar comparison)
  - [cmp_var_common](cmp_var_common.md) (signed comparison function)
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

## Simplified Source

```c
static int
cmp_abs_common(const NumericDigit *var1digits, int var1ndigits, int var1weight,
               const NumericDigit *var2digits, int var2ndigits, int var2weight)
{
    int i1 = 0;
    int i2 = 0;

    // Compare digits where var1 has higher weight
    while (var1weight > var2weight && i1 < var1ndigits) {
        if (var1digits[i1++] != 0)
            return 1; // var1 is larger
        var1weight--;
    }

    // Compare digits where var2 has higher weight
    while (var2weight > var1weight && i2 < var2ndigits) {
        if (var2digits[i2++] != 0)
            return -1; // var2 is larger
        var2weight--;
    }

    // Compare aligned digits (same weight)
    if (var1weight == var2weight) {
        while (i1 < var1ndigits && i2 < var2ndigits) {
            int stat = var1digits[i1++] - var2digits[i2++];

            if (stat) {
                if (stat > 0)
                    return 1;  // var1 digit is larger
                return -1;     // var2 digit is larger
            }
        }
    }

    // Check remaining digits in var1
    while (i1 < var1ndigits) {
        if (var1digits[i1++] != 0)
            return 1; // var1 has non-zero trailing digits
    }

    // Check remaining digits in var2
    while (i2 < var2ndigits) {
        if (var2digits[i2++] != 0)
            return -1; // var2 has non-zero trailing digits
    }

    return 0; // Numbers are equal
}
```
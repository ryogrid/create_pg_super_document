# cmp_var_common

## Location
[src/backend/utils/adt/numeric.c:8404-8446](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/numeric.c#L8404-L8446)

## Overview
Core comparison function that compares two numeric values represented by their constituent parts (digits, weight, sign) and returns their relative ordering.

## Definition

```c
static int
cmp_var_common(const NumericDigit *var1digits, int var1ndigits,
			   int var1weight, int var1sign,
			   const NumericDigit *var2digits, int var2ndigits,
			   int var2weight, int var2sign)
```
## Detailed Description
This function implements the core logic for comparing two numeric values by examining their constituent components rather than requiring complete NumericVar structures. It handles all comparison cases including zero values, sign differences, and magnitude comparisons. The function first handles special cases for zero values, then processes sign differences, and finally delegates to cmp_abs_common for magnitude comparison when both numbers have the same sign.

The comparison algorithm follows these rules: zero is handled specially, positive numbers are greater than negative numbers, and when signs are equal, absolute value comparison determines the result (with sign adjustment for negative numbers).

## Parameters / Member Variables
- : Pointer to digit array of first numeric value
- : Number of digits in first numeric value
- : Weight (power of NBASE) of most significant digit of first value
- : Sign of first numeric value (NUMERIC_POS or NUMERIC_NEG)
- : Pointer to digit array of second numeric value
- : Number of digits in second numeric value
- : Weight (power of NBASE) of most significant digit of second value
- : Sign of second numeric value (NUMERIC_POS or NUMERIC_NEG)

## Dependencies
- Functions called/Symbols referenced:
  - [cmp_abs_common](cmp_abs_common.md): Absolute value comparison function
  - NUMERIC_NEG: Constant for negative sign
  - NUMERIC_POS: Constant for positive sign
  - NumericDigit: Type for numeric digit storage

- Called from (representative examples):
  - NUMERIC_CAN_BE_SHORT: Numeric optimization checking
  - [cmp_numerics](cmp_numerics.md): High-level numeric comparison function
  - [cmp_var](cmp_var.md): NumericVar comparison wrapper function

## Notes and Other Information
- Returns negative value if first number < second number, zero if equal, positive if first > second
- Handles zero values as special cases (ndigits == 0 indicates zero)
- Uses sign-aware comparison logic: positive > negative regardless of magnitude
- For same-sign comparisons, delegates to cmp_abs_common for absolute value comparison
- When both numbers are negative, argument order is swapped for cmp_abs_common to handle sign inversion
- Designed to work with both NumericVar structures and raw Numeric data
- Forms the foundation for all numeric comparison operations in PostgreSQL
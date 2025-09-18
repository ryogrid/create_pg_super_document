# float8_qsort_cmp

## Location
src/backend/utils/adt/rangetypes_typanalyze.c: 95 - 111

## Overview
The  function is a comparison function specifically designed for sorting float8 (double precision) values, used primarily for sorting range lengths during statistical analysis.

## Definition


## Detailed Description
This is a standard three-way comparison function compatible with the qsort family of sorting functions. It compares two float8 values and returns an integer indicating their relative ordering. The function implements a straightforward numerical comparison suitable for sorting floating-point range lengths in ascending order.

The function follows the standard qsort comparison function contract, where the return value indicates whether the first argument is less than, equal to, or greater than the second argument.

## Parameters / Member Variables
- : Pointer to the first float8 value to compare
- : Pointer to the second float8 value to compare  
- : Unused argument required by qsort_arg interface (can be NULL)

## Dependencies
- Functions called/Symbols referenced:
  - None (uses only basic float8 operations)
- Called from:
  -  (used for sorting range length statistics)

## Notes and Other Information
- Returns -1 if the first value is less than the second
- Returns 0 if the values are equal
- Returns 1 if the first value is greater than the second
- Designed for use with qsort_arg() and similar sorting functions
- Specifically used in range statistics computation to sort range lengths for percentile calculations
- The function is declared static, making it internal to the rangetypes_typanalyze.c file
- Handles standard IEEE 754 float8 comparison semantics
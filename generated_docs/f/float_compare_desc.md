# float_compare_desc

## Location
src/backend/utils/adt/array_selfuncs.c: 1181 - 1192

## Overview
Comparison function for sorting float values in descending order, compatible with standard C library sorting functions.

## Definition
```c
static int float_compare_desc(const void *key1, const void *key2)
```

## Detailed Description
This function provides a simple comparison mechanism for floating-point values that produces descending (largest to smallest) sort order. It follows the standard comparison function contract used by qsort() and similar sorting routines. The function directly compares the float values after dereferencing the void pointers, returning appropriate integer values to indicate the relative ordering.

The function handles floating-point comparisons correctly, including proper handling of equal values, and provides a stable sorting order for descending arrangements.

## Parameters / Member Variables
- `key1`: Pointer to the first float value to compare (cast from const void*)
- `key2`: Pointer to the second float value to compare (cast from const void*)

## Dependencies
- Functions called/Symbols referenced:
  - (No external function calls - uses direct float comparison)
- Called from (representative examples):
  - EFFORT (array selectivity estimation context)

## Notes and Other Information
- Compatible with standard C library qsort() comparison function signature
- Returns -1 when first value is greater (for descending order)
- Returns 1 when first value is smaller (for descending order) 
- Returns 0 when values are equal
- Used in array statistics and selectivity estimation calculations
- Simple implementation without handling of special float values (NaN, infinity)
- Part of PostgreSQL's array analysis and query optimization infrastructure
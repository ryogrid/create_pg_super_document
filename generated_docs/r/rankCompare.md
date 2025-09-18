# rankCompare

## Location
src/bin/psql/crosstabview.c: 711 - 714

## Overview
A comparison function for 32-bit signed integers used to sort rank values in ascending order.

## Definition


## Detailed Description
This function provides a simple integer comparison for use with sorting algorithms in PostgreSQL's psql \crosstabview feature. It compares two 32-bit signed integers pointed to by the void pointers and returns the standard three-way comparison result. The function is specifically designed to work with qsort() and other comparison-based sorting algorithms to order rank values numerically.

The implementation delegates to PostgreSQL's internal pg_cmp_s32() utility function, which provides a safe and consistent comparison of 32-bit signed integers, handling edge cases and ensuring proper ordering semantics.

## Parameters / Member Variables
- `a`: Pointer to the first 32-bit signed integer to compare
- `b`: Pointer to the second 32-bit signed integer to compare

## Dependencies
- Functions called/Symbols referenced:
  - pg_cmp_s32 (PostgreSQL's 32-bit signed integer comparison utility)
- Called from (representative examples):
  - rankSort (for sorting rank values in pivot field processing)

## Notes and Other Information
- Returns negative value if *a < *b, zero if *a == *b, positive value if *a > *b
- Used specifically by rankSort() to order columns by their numeric rank values
- Compatible with standard library qsort() function signature requirements
- Provides ascending order sorting (smallest ranks first)
- Leverages PostgreSQL's internal comparison utilities for consistency and safety
- Part of the crosstab column ordering mechanism that allows custom arrangement of pivot columns
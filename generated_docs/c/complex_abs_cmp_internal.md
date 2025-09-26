# complex_abs_cmp_internal

## Location
src/tutorial/complex.c: 132 - 147

## Overview
A static internal comparison function that compares the absolute values (magnitudes) of two complex numbers and returns a three-way comparison result (-1, 0, or 1).

## Definition


## Detailed Description
This function serves as the core comparison logic for all B-tree index operators on complex numbers. It computes the magnitude (absolute value) of each complex number using the Pythagorean theorem and performs a three-way comparison. The function is designed to ensure consistent ordering across all comparison operators in the B-tree index opclass, reducing the risk of inconsistent comparison functions by centralizing the comparison logic.

The magnitude is calculated as x² + y² for each complex number, avoiding the square root operation since relative ordering is preserved without it.

## Parameters / Member Variables
- `a`: Pointer to the first Complex number to compare
- `b`: Pointer to the second Complex number to compare

## Dependencies
- Functions called/Symbols referenced:
  - Mag (macro for calculating magnitude as x² + y²)
  - Complex (struct type for complex numbers)
- Called from (representative examples):
  - complex_abs_lt
  - complex_abs_le
  - complex_abs_eq
  - complex_abs_ge
  - complex_abs_gt
  - complex_abs_cmp

## Notes and Other Information
- This is a static function, only accessible within the complex.c file
- Returns -1 if |a| < |b|, 0 if |a| = |b|, and 1 if |a| > |b|
- Part of the B-tree index operator class implementation for complex numbers
- Avoids computing actual square root for performance, using x² + y² comparison instead
- Located in src/tutorial/complex.c:132-147
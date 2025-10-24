# complex_abs_cmp_internal

## Location
[src/tutorial/complex.c:132-147](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/tutorial/complex.c#L132-L147)

## Overview
A static internal comparison function that compares the absolute values (magnitudes) of two complex numbers and returns a three-way comparison result (-1, 0, or 1).

## Definition

```c
static int
complex_abs_cmp_internal(Complex * a, Complex * b)
```
## Detailed Description
This function serves as the core comparison logic for all B-tree index operators on complex numbers. It computes the magnitude (absolute value) of each complex number using the Pythagorean theorem and performs a three-way comparison. The function is designed to ensure consistent ordering across all comparison operators in the B-tree index opclass, reducing the risk of inconsistent comparison functions by centralizing the comparison logic.

The magnitude is calculated as x² + y² for each complex number, avoiding the square root operation since relative ordering is preserved without it.

## Parameters / Member Variables
- `a`: Pointer to the first Complex number to compare
- `b`: Pointer to the second Complex number to compare

## Dependencies
- Functions called/Symbols referenced:
  - Mag (macro for calculating magnitude as x² + y²)
  - [Complex](../C/Complex.md) (struct type for complex numbers)
- Called from (representative examples):
  - [complex_abs_lt](complex_abs_lt.md)
  - [complex_abs_le](complex_abs_le.md)
  - [complex_abs_eq](complex_abs_eq.md)
  - [complex_abs_ge](complex_abs_ge.md)
  - [complex_abs_gt](complex_abs_gt.md)
  - [complex_abs_cmp](complex_abs_cmp.md)

## Notes and Other Information
- This is a static function, only accessible within the complex.c file
- Returns -1 if |a| < |b|, 0 if |a| = |b|, and 1 if |a| > |b|
- Part of the B-tree index operator class implementation for complex numbers
- Avoids computing actual square root for performance, using x² + y² comparison instead
- Located in src/tutorial/complex.c:132-147

## Simplified Source

```c
static int complex_abs_cmp_internal(Complex * a, Complex * b) {
    // Calculate magnitudes (x² + y²) for both complex numbers
    double amag = Mag(a),
           bmag = Mag(b);

    // Three-way comparison of magnitudes
    if (amag < bmag)
        return -1;
    if (amag > bmag)
        return 1;
    return 0;
}
```
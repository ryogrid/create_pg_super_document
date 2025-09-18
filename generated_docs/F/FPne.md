# FPne

## Location
[src/include/utils/geo_decls.h:53-58](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L53-L58)

## Overview
FPne is a static inline function that performs floating-point inequality comparison with epsilon tolerance, designed to determine when two floating-point values are significantly different in geometric calculations.

## Definition

static inline bool
FPne(double A, double B)
{
    return A != B && fabs(A - B) > EPSILON;
}

## Detailed Description
FPne implements a fuzzy inequality comparison for double-precision floating-point numbers. The function performs a two-stage check: first it uses direct inequality comparison (A != B) to quickly identify obviously different values, then it verifies that the absolute difference exceeds the EPSILON threshold. This ensures that only truly significant differences are reported as inequalities, filtering out small differences that could result from floating-point precision limitations. This is the logical complement of FPeq and is essential for reliable geometric computations.

## Parameters / Member Variables
- A: First double-precision floating-point value to compare
- B: Second double-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - EPSILON (constant defining the tolerance threshold)
  - fabs (standard library function for absolute value)
- Called from (representative examples):
  - [circle_ne](../c/circle_ne.md)

## Notes and Other Information
This function is the complement of FPeq and is used less frequently in PostgreSQL's geometric operations. The dual condition (A != B && fabs(A - B) > EPSILON) ensures that both the direct inequality check passes and the difference is meaningful beyond floating-point precision errors. This approach prevents false positives where tiny rounding errors might otherwise be interpreted as significant differences.
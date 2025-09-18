# FPeq

## Location
[src/include/utils/geo_decls.h:47-52](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L47-L52)

## Overview
FPeq is a static inline function that performs floating-point equality comparison with epsilon tolerance, designed to handle floating-point precision issues in geometric calculations.

## Definition


## Detailed Description
FPeq implements a fuzzy equality comparison for double-precision floating-point numbers. The function first performs a direct equality check (A == B), which handles cases where both values are exactly equal (including special values like infinity or NaN). If the direct comparison fails, it falls back to an epsilon-based comparison using the absolute difference between the two values. This approach is essential in geometric computations where floating-point arithmetic can introduce small rounding errors that would make exact equality comparisons unreliable.

## Parameters / Member Variables
- `A`: First double-precision floating-point value to compare
- `B`: Second double-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - EPSILON (constant defining the tolerance threshold)
  - fabs (standard library function for absolute value)
- Called from (representative examples):
  - [gist_point_consistent_internal](../g/gist_point_consistent_internal.md)
  - [box_eq](../b/box_eq.md)
  - [line_eq](../l/line_eq.md)
  - [point_eq_point](../p/point_eq_point.md)
  - [circle_eq](../c/circle_eq.md)
  - [lseg_parallel](../l/lseg_parallel.md)

## Notes and Other Information
This function is part of PostgreSQL's geometric data type infrastructure and is extensively used throughout the geometric operations module. The epsilon-based comparison is crucial for reliable geometric computations where floating-point precision limitations could otherwise cause incorrect results. The function is defined as static inline in the header file for optimal performance in frequently called geometric operations.
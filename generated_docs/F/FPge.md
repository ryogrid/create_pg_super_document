# FPge

## Location
src/include/utils/geo_decls.h: 77 - 81

## Overview
FPge is an inline utility function that performs floating-point greater-than-or-equal comparison with epsilon tolerance for geometric operations in PostgreSQL.

## Definition


## Detailed Description
The FPge function implements a floating-point comparison that accounts for numerical precision issues inherent in floating-point arithmetic. Instead of performing a direct comparison (A >= B), it adds a small epsilon value (EPSILON) to the first operand before comparison. This approach helps handle cases where floating-point rounding errors might cause mathematically equivalent values to be considered unequal. The function is widely used throughout PostgreSQL's geometric operations to ensure reliable comparisons for spatial data types.

## Parameters / Member Variables
- : First double-precision floating-point value to compare
- : Second double-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - EPSILON (constant for floating-point tolerance)
- Called from (representative examples):
  - gist_point_consistent_internal
  - box_overright
  - box_overabove
  - box_contain_box
  - box_above_eq
  - box_ge
  - lseg_ge
  - circle_overright
  - circle_overabove
  - circle_ge
  - lseg_crossing
  - overlap2D
  - contain2D
  - contained2D
  - overHigher2D

## Notes and Other Information
This function is defined as a static inline function in src/include/utils/geo_decls.h:77-81, making it available for efficient inlining throughout the codebase. It is extensively used in geometric comparison operations, spatial indexing (GiST and SP-GiST), and various geometric predicates. The epsilon-based comparison is crucial for the reliability of PostgreSQL's geometric data types including points, boxes, line segments, and circles.
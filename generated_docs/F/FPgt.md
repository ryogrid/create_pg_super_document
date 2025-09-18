# FPgt

## Location
src/include/utils/geo_decls.h: 71 - 76

## Overview
FPgt is a static inline function that performs floating-point greater-than comparison with epsilon tolerance, designed to handle floating-point precision issues in geometric ordering operations.

## Definition

static inline bool
FPgt(double A, double B)
{
    return A > B + EPSILON;
}

## Detailed Description
FPgt implements a fuzzy greater-than comparison for double-precision floating-point numbers. The function compares A against B + EPSILON, meaning that A must exceed B by more than the EPSILON threshold to be considered greater. This approach ensures that values which are very close to each other (within EPSILON tolerance) are not considered to be in a greater-than relationship, thus providing stable ordering that accounts for floating-point precision limitations. This is the complement to FPlt and is essential for consistent geometric computations where reliable ordering relationships are required.

## Parameters / Member Variables
- A: First double-precision floating-point value (left operand of comparison)
- B: Second double-precision floating-point value (right operand of comparison)

## Dependencies
- Functions called/Symbols referenced:
  - EPSILON (constant defining the tolerance threshold)
- Called from (representative examples):
  - gist_point_consistent_internal
  - spg_kd_inner_consistent
  - box_right
  - box_above
  - point_right
  - circle_gt
  - lseg_crossing
  - higher2D

## Notes and Other Information
This function is widely used throughout PostgreSQL's geometric operations, particularly in spatial indexing (GiST and SP-GiST) and geometric comparisons. The epsilon-adjusted comparison (A > B + EPSILON) means that A must be meaningfully greater than B, not just different due to floating-point precision errors. This ensures stable and consistent ordering behavior that is crucial for geometric algorithms and spatial data structures. The function is the logical complement of FPlt, providing the opposite comparison direction with the same epsilon-based tolerance.
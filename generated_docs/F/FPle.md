# FPle

## Location
src/include/utils/geo_decls.h: 65 - 70

## Overview
FPle is a static inline function that performs floating-point less-than-or-equal comparison with epsilon tolerance, designed to handle floating-point precision issues in geometric ordering and containment operations.

## Definition

static inline bool
FPle(double A, double B)
{
    return A <= B + EPSILON;
}

## Detailed Description
FPle implements a fuzzy less-than-or-equal comparison for double-precision floating-point numbers. The function compares A against B + EPSILON, effectively creating a tolerance zone where values that are very close to each other (within EPSILON) are considered to be in a less-than-or-equal relationship. This approach is crucial for geometric operations where containment, overlap, and boundary conditions need to be evaluated reliably despite floating-point precision limitations. The function serves as the complement to FPlt for inclusive comparisons.

## Parameters / Member Variables
- A: First double-precision floating-point value (left operand of comparison)
- B: Second double-precision floating-point value (right operand of comparison)

## Dependencies
- Functions called/Symbols referenced:
  - EPSILON (constant defining the tolerance threshold)
- Called from (representative examples):
  - [gist_point_consistent_internal](../g/gist_point_consistent_internal.md)
  - [box_ov](../b/box_ov.md)
  - [box_overleft](../b/box_overleft.md)
  - [box_contain_box](../b/box_contain_box.md)
  - [circle_overlap](../c/circle_overlap.md)
  - [circle_contained](../c/circle_contained.md)
  - [overlap2D](../o/overlap2D.md)
  - [contain2D](../c/contain2D.md)

## Notes and Other Information
This function is extensively used in PostgreSQL's geometric operations, particularly for containment and overlap checks. The epsilon-adjusted comparison (A <= B + EPSILON) allows for inclusive relationships that account for floating-point precision errors. This is especially important in spatial indexing and geometric containment operations where boundary conditions must be handled reliably. The function ensures that values that are effectively equal (within EPSILON tolerance) are treated as satisfying the less-than-or-equal condition.
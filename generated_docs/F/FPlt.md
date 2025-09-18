# FPlt

## Location
src/include/utils/geo_decls.h: 59 - 64

## Overview
FPlt is a static inline function that performs floating-point less-than comparison with epsilon tolerance, designed to handle floating-point precision issues in geometric ordering operations.

## Definition

static inline bool
FPlt(double A, double B)
{
    return A + EPSILON < B;
}

## Detailed Description
FPlt implements a fuzzy less-than comparison for double-precision floating-point numbers. Rather than using a simple A < B comparison, the function adds EPSILON to A before comparing with B. This approach ensures that values which are very close to each other (within EPSILON tolerance) are not considered to be in a less-than relationship, thus providing a stable ordering that accounts for floating-point precision limitations. This is essential in geometric computations where consistent ordering relationships are required despite potential rounding errors.

## Parameters / Member Variables
- A: First double-precision floating-point value (left operand of comparison)
- B: Second double-precision floating-point value (right operand of comparison)

## Dependencies
- Functions called/Symbols referenced:
  - EPSILON (constant defining the tolerance threshold)
- Called from (representative examples):
  - [gist_point_consistent_internal](../g/gist_point_consistent_internal.md)
  - [spg_kd_inner_consistent](../s/spg_kd_inner_consistent.md)
  - [box_left](../b/box_left.md)
  - [box_below](../b/box_below.md)
  - [point_left](../p/point_left.md)
  - [circle_lt](../c/circle_lt.md)
  - [lseg_crossing](../l/lseg_crossing.md)

## Notes and Other Information
This function is widely used throughout PostgreSQL's geometric operations, particularly in spatial indexing (GiST and SP-GiST) and geometric comparisons. The epsilon-adjusted comparison (A + EPSILON < B) means that A must be meaningfully smaller than B, not just different due to floating-point precision errors. This ensures stable and consistent ordering behavior that is crucial for geometric algorithms and spatial data structures.
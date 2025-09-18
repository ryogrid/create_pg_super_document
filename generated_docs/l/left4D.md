# left4D

## Location
src/backend/utils/adt/geo_spgist.c: 318 - 324

## Overview
Determines if any rectangle from a RectBox can be positioned to the left of a given query boundary.

## Definition


## Detailed Description
This function is part of PostgreSQL's SP-GiST (Space-Partitioned Generalized Search Tree) implementation for geometric box operations. It evaluates whether any rectangle within the provided RectBox structure could potentially be positioned entirely to the left of the specified query boundary. The function operates by comparing the x-axis range of the rectangle box with the left boundary of the query range using the lower2D helper function.

## Parameters / Member Variables
- : Pointer to RectBox structure containing the spatial boundaries to be evaluated
- : Pointer to RangeBox structure defining the query boundary conditions

## Dependencies
- Functions called/Symbols referenced:
  - lower2D
  - RectBox (struct)
  - RangeBox (struct)
- Called from (representative examples):
  - spg_box_quad_inner_consistent

## Notes and Other Information
This function is used in SP-GiST index operations for spatial queries involving box geometries. It specifically checks the x-axis positioning relationship and is part of a set of directional predicates (left4D, right4D, overLeft4D, overRight4D, below4D) used for efficient spatial indexing and query processing. The function is declared static, indicating it's only used within the geo_spgist.c file.
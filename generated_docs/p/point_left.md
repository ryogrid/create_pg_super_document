# point_left

## Location
src/backend/utils/adt/geo_ops.c: 1901 - 1909

## Overview
A PostgreSQL function that determines if one point is to the left of another point by comparing their x-coordinates.

## Definition


## Detailed Description
This function is part of PostgreSQL's geometric data type operations for 2D points. It implements a relational operator that compares two Point structures to determine if the first point is positioned to the left of the second point in a 2D coordinate system. The comparison is based solely on the x-coordinates of the points, using floating-point comparison with appropriate epsilon handling for accuracy.

The function is part of a family of relational operators for Points that maintain consistency with PostgreSQL's coordinate equality concepts (such as point_vert and point_horiz), which means results may have some tolerance based on the EPSILON value used in floating-point comparisons.

## Parameters / Member Variables
- : First Point argument (left operand) - retrieved using PG_GETARG_POINT_P(0)
- : Second Point argument (right operand) - retrieved using PG_GETARG_POINT_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - Point (data type)
  - PG_GETARG_POINT_P (macro for extracting Point arguments)
  - FPlt (floating-point less-than comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from (representative examples):
  - getQuadrant (in spgquadtreeproc.c)
  - spg_quad_inner_consistent (in spgquadtreeproc.c)
  - spg_quad_leaf_consistent (in spgquadtreeproc.c)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1901-1909
- Part of PostgreSQL's geometric operators that preserve coordinate equality sense
- Results may be approximate due to floating-point epsilon handling
- Primarily used in spatial indexing operations, particularly SP-GiST quadtree implementations
- Returns true if pt1->x < pt2->x using floating-point comparison
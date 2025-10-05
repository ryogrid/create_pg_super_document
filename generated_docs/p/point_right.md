# point_right

## Location
[src/backend/utils/adt/geo_ops.c:1910-1918](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1910-L1918)

## Overview
A PostgreSQL function that determines if one point is to the right of another point by comparing their x-coordinates.

## Definition
```c
Datum point_right(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's geometric data type operations for 2D points. It implements a relational operator that compares two Point structures to determine if the first point is positioned to the right of the second point in a 2D coordinate system. The comparison is based solely on the x-coordinates of the points, using floating-point comparison with appropriate epsilon handling for accuracy.

The function is the complement to point_left and is part of a family of relational operators for Points that maintain consistency with PostgreSQL's coordinate equality concepts, which means results may have some tolerance based on the EPSILON value used in floating-point comparisons.

## Parameters / Member Variables
- `pt1`: First Point argument (left operand) - retrieved using PG_GETARG_POINT_P(0)
- `pt2`: Second Point argument (right operand) - retrieved using PG_GETARG_POINT_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) (data type)
  - PG_GETARG_POINT_P (macro for extracting Point arguments)
  - [FPgt](../F/FPgt.md) (floating-point greater-than comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from (representative examples):
  - [getQuadrant](../g/getQuadrant.md) (in spgquadtreeproc.c)
  - [spg_quad_inner_consistent](../s/spg_quad_inner_consistent.md) (in spgquadtreeproc.c)
  - [spg_quad_leaf_consistent](../s/spg_quad_leaf_consistent.md) (in spgquadtreeproc.c)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1910-1918
- Part of PostgreSQL's geometric operators that preserve coordinate equality sense
- Results may be approximate due to floating-point epsilon handling
- Primarily used in spatial indexing operations, particularly SP-GiST quadtree implementations
- Returns true if pt1->x > pt2->x using floating-point comparison
- Complementary function to point_left for horizontal position comparisons

## Simplified Source
```c
Datum point_right(PG_FUNCTION_ARGS) {
    Point *pt1 = PG_GETARG_POINT_P(0);
    Point *pt2 = PG_GETARG_POINT_P(1);

    // Return true if pt1 is to the right of pt2 (larger x-coordinate)
    PG_RETURN_BOOL(FPgt(pt1->x, pt2->x));
}
```
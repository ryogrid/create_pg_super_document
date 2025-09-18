# point_vert

## Location
src/backend/utils/adt/geo_ops.c: 1937 - 1945

## Overview
A PostgreSQL function that determines if two points are vertically aligned by comparing their x-coordinates for equality.

## Definition
```c
Datum point_vert(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is part of PostgreSQL's geometric data type operations for 2D points. It implements a relational operator that compares two Point structures to determine if they are vertically aligned, meaning they share the same x-coordinate. The comparison uses floating-point equality with appropriate epsilon handling to account for floating-point precision issues.

Unlike the directional comparison functions (point_left, point_right, point_above, point_below), this function checks for coordinate alignment rather than relative position. It forms a foundational equality concept mentioned in the comments of related geometric operators, where coordinates are considered "equal" within a given accuracy tolerance.

## Parameters / Member Variables
- `pt1`: First Point argument (left operand) - retrieved using PG_GETARG_POINT_P(0)
- `pt2`: Second Point argument (right operand) - retrieved using PG_GETARG_POINT_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) (data type)
  - PG_GETARG_POINT_P (macro for extracting Point arguments)
  - [FPeq](../F/FPeq.md) (floating-point equality comparison function)
  - PG_RETURN_BOOL (macro for returning boolean result)
- Called from (representative examples):
  - [getQuadrant](../g/getQuadrant.md) (in spgquadtreeproc.c)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1937-1945
- Part of PostgreSQL's geometric operators that define coordinate equality concepts
- Results may be approximate due to floating-point epsilon handling (EPSILON tolerance)
- Used in spatial indexing operations, particularly SP-GiST quadtree implementations
- Returns true if pt1->x == pt2->x using floating-point equality comparison
- Provides vertical alignment detection, complementing point_horiz (horizontal alignment)
- Forms the basis for coordinate equality concepts referenced in other geometric operators
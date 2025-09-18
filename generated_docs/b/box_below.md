# box_below

## Location
src/backend/utils/adt/geo_ops.c: 635 - 646

## Overview
The box_below function determines if one box is strictly below another box by comparing their y-coordinates.

## Definition
```c
Datum box_below(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a geometric comparison operation that checks whether box1 is positioned entirely below box2. The comparison is performed by checking if the highest y-coordinate of box1 is less than the lowest y-coordinate of box2. This ensures that there is no vertical overlap between the boxes, with box1 being completely below box2.

The function uses PostgreSQL's floating-point comparison function FPlt() to handle potential precision issues when comparing coordinates. This function operates on the y-axis, complementing the horizontal positioning functions (box_left, box_right, etc.).

## Parameters / Member Variables
- `box1` (BOX*): The first box argument, obtained via PG_GETARG_BOX_P(0)
- `box2` (BOX*): The second box argument, obtained via PG_GETARG_BOX_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro for extracting BOX arguments)
  - FPlt (floating-point less-than comparison)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - BOX (geometric box type)
- Called from (representative examples):
  - gist_box_leaf_consistent (GiST index consistency checking)
  - rtree_internal_consistent (R-tree internal node consistency)
  - spg_box_quad_leaf_consistent (SP-GiST quad-tree leaf consistency)

## Notes and Other Information
- This is one of the directional positioning operators for PostgreSQL's box geometric type
- The function operates on the vertical axis (y-coordinates) rather than horizontal (x-coordinates)
- The function is used in spatial indexing operations, particularly in GiST and SP-GiST index implementations
- Returns true only when box1 is completely below box2 with no vertical overlap
- Part of the complete set of directional positioning functions for 2D spatial queries
- Located in src/backend/utils/adt/geo_ops.c:635-646
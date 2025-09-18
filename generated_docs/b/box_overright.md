# box_overright

## Location
src/backend/utils/adt/geo_ops.c: 624 - 634

## Overview
The box_overright function determines if the left edge of box1 is at or to the right of the left edge of box2, implementing a "greater than or equal" comparison for spatial positioning.

## Definition
```c
Datum box_overright(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a geometric comparison operation that checks whether box1 does not start further left than box2. Unlike box_right which requires complete separation, box_overright allows for overlap as long as box1's leftmost edge is not to the left of box2's leftmost edge.

The comparison is performed by checking if the lowest x-coordinate of box1 is greater than or equal to the lowest x-coordinate of box2. This operation is particularly useful when working with time ranges stored as rectangles, where it represents a "greater than or equal" relationship for the start of time ranges.

The function uses PostgreSQL's floating-point comparison function FPge() to handle potential precision issues when comparing coordinates. This is the complementary function to box_overleft.

## Parameters / Member Variables
- `box1` (BOX*): The first box argument, obtained via PG_GETARG_BOX_P(0)
- `box2` (BOX*): The second box argument, obtained via PG_GETARG_BOX_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro for extracting BOX arguments)
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - [BOX](../B/BOX.md) (geometric box type)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (R-tree internal node consistency)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST quad-tree leaf consistency)

## Notes and Other Information
- This is one of the directional positioning operators for PostgreSQL's box geometric type
- Unlike box_right, this function allows for overlap between boxes
- Specifically designed for temporal range comparisons when time ranges are represented as rectangles
- The function is used in spatial indexing operations, particularly in GiST and SP-GiST index implementations
- Returns true when box1's left edge does not start before box2's left edge
- Complementary to the box_overleft function, providing the opposite directional comparison
- Located in src/backend/utils/adt/geo_ops.c:624-634
# box_right

## Location
[src/backend/utils/adt/geo_ops.c:609-623](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L609-L623)

## Overview
The box_right function determines if one box is strictly to the right of another box by comparing their x-coordinates.

## Definition
```c
Datum box_right(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a geometric comparison operation that checks whether box1 is positioned entirely to the right of box2. The comparison is performed by checking if the lowest x-coordinate of box1 is greater than the highest x-coordinate of box2. This ensures that there is no horizontal overlap between the boxes, with box1 being completely to the right of box2.

The function uses PostgreSQL's floating-point comparison function FPgt() to handle potential precision issues when comparing coordinates. This is the directional opposite of the box_left function.

## Parameters / Member Variables
- `box1` (BOX*): The first box argument, obtained via PG_GETARG_BOX_P(0)
- `box2` (BOX*): The second box argument, obtained via PG_GETARG_BOX_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro for extracting BOX arguments)
  - [FPgt](../F/FPgt.md) (floating-point greater-than comparison)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - [BOX](../B/BOX.md) (geometric box type)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (R-tree internal node consistency)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST quad-tree leaf consistency)

## Notes and Other Information
- This is one of the directional positioning operators for PostgreSQL's box geometric type
- The function is used in spatial indexing operations, particularly in GiST and SP-GiST index implementations
- Returns true only when box1 is completely to the right of box2 with no horizontal overlap
- Complementary to the box_left function, providing the opposite directional comparison
- Located in src/backend/utils/adt/geo_ops.c:609-623
# box_overleft

## Location
[src/backend/utils/adt/geo_ops.c:598-608](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L598-L608)

## Overview
The box_overleft function determines if the right edge of box1 is at or to the left of the right edge of box2, implementing a "less than or equal" comparison for spatial positioning.

## Definition
```c
Datum box_overleft(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements a geometric comparison operation that checks whether box1 does not extend further right than box2. Unlike box_left which requires complete separation, box_overleft allows for overlap as long as box1's rightmost edge does not exceed box2's rightmost edge.

The comparison is performed by checking if the highest x-coordinate of box1 is less than or equal to the highest x-coordinate of box2. This operation is particularly useful when working with time ranges stored as rectangles, where it represents a "less than or equal" relationship for the end of time ranges.

The function uses PostgreSQL's floating-point comparison function FPle() to handle potential precision issues when comparing coordinates.

## Parameters / Member Variables
- `box1` (BOX*): The first box argument, obtained via PG_GETARG_BOX_P(0)
- `box2` (BOX*): The second box argument, obtained via PG_GETARG_BOX_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_BOX_P (macro for extracting BOX arguments)
  - [FPle](../F/FPle.md) (floating-point less-than-or-equal comparison)
  - PG_RETURN_BOOL (macro for returning boolean result)
  - [BOX](../B/BOX.md) (geometric box type)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (R-tree internal node consistency)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST quad-tree leaf consistency)

## Notes and Other Information
- This is one of the directional positioning operators for PostgreSQL's box geometric type
- Unlike box_left, this function allows for overlap between boxes
- Specifically designed for temporal range comparisons when time ranges are represented as rectangles
- The function is used in spatial indexing operations, particularly in GiST and SP-GiST index implementations
- Returns true when box1's right edge does not extend beyond box2's right edge
- Located in src/backend/utils/adt/geo_ops.c:598-608

## Simplified Source

```c
Datum box_overleft(PG_FUNCTION_ARGS) {
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Check if box1's right edge is at or left of box2's right edge
    // This means box1 doesn't extend further right than box2
    PG_RETURN_BOOL(box1->high.x <= box2->high.x);
}
```
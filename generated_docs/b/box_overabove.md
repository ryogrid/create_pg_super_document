# box_overabove

## Location
[src/backend/utils/adt/geo_ops.c:670-680](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L670-L680)

## Overview
Tests whether the lower edge of the first box is at or above the lower edge of the second box in PostgreSQL's geometric box operations.

## Definition

```c
Datum
box_overabove(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements a geometric relationship test that determines if the lower edge (minimum y-coordinate) of the first box is positioned at or above the lower edge of the second box. This function checks whether box1->low.y >= box2->low.y, allowing for cases where the boxes may overlap vertically but ensuring that the bottom of the first box is not below the bottom of the second box.

This spatial relationship operator is particularly useful in spatial indexing and geometric analysis operations where relative positioning of box boundaries is important. Unlike the strict  function,  allows for partial or complete overlap as long as the lower boundary condition is satisfied.

## Parameters / Member Variables
- : The first BOX object whose lower edge is being tested
- : The second BOX object used as the reference for comparison

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (data type structure)
  - PG_GETARG_BOX_P (macro for extracting box arguments)
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (R-tree index consistency checking)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST index consistency checking)

## Notes and Other Information
- The comparison specifically checks if box1->low.y >= box2->low.y
- Uses floating-point comparison to handle potential precision issues in coordinate values
- Part of PostgreSQL's comprehensive set of geometric relationship operators for box data types
- Commonly used in spatial indexing operations, particularly in GiST and SP-GiST implementations
- Allows for overlapping boxes unlike the strict  operator
- The "over" prefix indicates that equality is included in the comparison (>= rather than >)
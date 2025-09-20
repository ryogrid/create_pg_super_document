# box_above

## Location
[src/backend/utils/adt/geo_ops.c:658-669](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L658-L669)

## Overview
Tests whether the first box is strictly above the second box in PostgreSQL's geometric box operations.

## Definition

```c
Datum
box_above(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements a geometric relationship test that determines if one box is positioned strictly above another box. It checks whether the lower edge (minimum y-coordinate) of the first box is greater than the upper edge (maximum y-coordinate) of the second box. This ensures there is no vertical overlap between the boxes and that box1 is entirely above box2.

This function is a fundamental spatial relationship operator used in PostgreSQL's geometric data types for spatial indexing, query optimization, and geometric analysis operations. The strict comparison ensures that boxes that merely touch at their edges are not considered to be "above" each other.

## Parameters / Member Variables
- : The first BOX object being tested to see if it is above the second box
- : The second BOX object used as the reference for the comparison

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (data type structure)
  - PG_GETARG_BOX_P (macro for extracting box arguments)
  - [FPgt](../F/FPgt.md) (floating-point greater-than comparison)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (R-tree index consistency checking)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST index consistency checking)

## Notes and Other Information
- This function performs a strict comparison: box1->low.y > box2->high.y
- Used extensively in spatial indexing operations, particularly in GiST and SP-GiST implementations
- The comparison uses floating-point operations to handle coordinate precision issues
- Part of a comprehensive set of geometric relationship operators for PostgreSQL's box data type
- Ensures no vertical overlap exists between the two boxes being compared
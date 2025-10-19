# box_contain

## Location
[src/backend/utils/adt/geo_ops.c:692-703](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L692-L703)

## Overview
Tests whether the first box completely contains the second box in PostgreSQL's geometric box operations.

## Definition

```c
Datum
box_contain(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements a geometric containment test that determines if one box (box1) completely contains another box (box2). This function serves as a wrapper around the internal  function, calling it with the arguments in their natural order to check if box1 contains box2.

This containment relationship is a fundamental spatial operation used extensively in spatial indexing, query optimization, and geometric analysis. For box1 to contain box2, all points of box2 must lie within or on the boundaries of box1. This is the complement of the  function which tests the reverse relationship.

## Parameters / Member Variables
- : The BOX object being tested to see if it contains box2
- : The BOX object that is potentially contained within box1

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (data type structure)
  - PG_GETARG_BOX_P (macro for extracting box arguments)
  - [box_contain_box](box_contain_box.md) (internal function that performs the actual containment test)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [rtree_internal_consistent](../r/rtree_internal_consistent.md) (R-tree index consistency checking)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST index consistency checking)

## Notes and Other Information
- This function is a direct wrapper around  with arguments in natural order
- Used extensively in spatial indexing operations, particularly in GiST and SP-GiST implementations
- The containment test verifies that box2 fits entirely within box1's boundaries
- Part of PostgreSQL's comprehensive set of geometric relationship operators for box data types
- Implements the "contains" operator (~) in PostgreSQL's geometric operations
- Complementary to the  function which tests the opposite relationship
- Critical for spatial query optimization and index traversal algorithms

## Simplified Source

```c
Datum box_contain(PG_FUNCTION_ARGS) {
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Check if box1 contains box2
    // Uses the internal box_contain_box function
    PG_RETURN_BOOL(box_contain_box(box1, box2));
}
```
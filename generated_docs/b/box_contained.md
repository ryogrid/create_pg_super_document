# box_contained

## Location
[src/backend/utils/adt/geo_ops.c:681-691](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L681-L691)

## Overview
Tests whether the first box is completely contained within the second box in PostgreSQL's geometric box operations.

## Definition

```c
Datum
box_contained(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements a geometric containment test that determines if one box (box1) is completely contained within another box (box2). This function serves as a wrapper that calls the internal  function with the arguments reversed - it checks if box2 contains box1 by calling .

This containment relationship is fundamental in spatial operations and is used extensively in spatial indexing, query optimization, and geometric analysis. For box1 to be contained in box2, all points of box1 must lie within or on the boundaries of box2.

## Parameters / Member Variables
- : The BOX object being tested to see if it is contained within box2
- : The BOX object that potentially contains box1

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (data type structure)
  - PG_GETARG_BOX_P (macro for extracting box arguments)
  - [box_contain_box](box_contain_box.md) (internal function that performs the actual containment test)
  - PG_RETURN_BOOL (macro for returning boolean results)
- Called from (representative examples):
  - [gist_box_leaf_consistent](../g/gist_box_leaf_consistent.md) (GiST index consistency checking)
  - [spg_box_quad_leaf_consistent](../s/spg_box_quad_leaf_consistent.md) (SP-GiST index consistency checking)

## Notes and Other Information
- This function is essentially a wrapper around  with reversed argument order
- Used in spatial indexing operations, particularly in GiST and SP-GiST implementations
- The containment test checks that box1 fits entirely within box2's boundaries
- Part of PostgreSQL's comprehensive set of geometric relationship operators for box data types
- The function implements the "contained by" operator (@) in PostgreSQL's geometric operations
- Complementary to the  function which tests the opposite relationship

## Simplified Source

```c
Datum box_contained(PG_FUNCTION_ARGS) {
    BOX *box1 = PG_GETARG_BOX_P(0);
    BOX *box2 = PG_GETARG_BOX_P(1);

    // Check if box1 is contained within box2
    // This reverses the arguments to box_contain_box
    PG_RETURN_BOOL(box_contain_box(box2, box1));
}
```
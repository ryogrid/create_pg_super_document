# getRangeBox

## Location
[src/backend/utils/adt/geo_spgist.c:157-176](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L157-L176)

## Overview
Converts a PostgreSQL BOX geometric type into a RangeBox structure to represent the box as points in 4D space for SP-GiST indexing operations.

## Definition

```c
static RangeBox *
getRangeBox(BOX *box)
```
## Detailed Description
This function performs a structural transformation from PostgreSQL's standard BOX representation to a RangeBox representation used internally by the geometric SP-GiST implementation. The transformation emphasizes the functional role of representing geometric boxes as points in 4-dimensional space, where each dimension corresponds to the low and high values of the x and y coordinates.

The RangeBox structure provides a more convenient access pattern for SP-GiST operations by organizing the coordinate ranges into left (x-axis) and right (y-axis) Range structures, each containing low and high float8 values. This representation facilitates range-based comparisons and spatial operations required by the indexing algorithms.

## Parameters / Member Variables
- `*box`: Pointer to a BOX structure containing low and high points with x,y coordinates
## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (PostgreSQL geometric box type)
  - [RangeBox](../R/RangeBox.md) (4D range representation structure)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- This is a static function, accessible only within geo_spgist.c
- Allocates memory for the RangeBox using palloc, which integrates with PostgreSQL's memory management
- The transformation maps: box->low.x to range_box->left.low, box->high.x to range_box->left.high, box->low.y to range_box->right.low, box->high.y to range_box->right.high
- Part of the geometric SP-GiST indexing infrastructure that treats 2D boxes as 4D points for efficient spatial indexing
- The RangeBox structure consists of two Range structures (left and right), each containing low and high float8 values
- This abstraction helps in quadrant-based spatial partitioning where boxes are compared based on their coordinate ranges
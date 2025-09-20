# initRectBox

## Location
[src/backend/utils/adt/geo_spgist.c:177-204](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L177-L204)

## Overview
Initializes a RectBox structure to represent the entire 4D space with no restrictions for SP-GiST index traversal operations.

## Definition

```c
static RectBox *
initRectBox(void)
```
## Detailed Description
This function creates and initializes a RectBox structure that represents the entire 4-dimensional space by setting all range boundaries to infinity. It serves as the starting point for SP-GiST index traversal operations where no spatial restrictions have been applied yet.

The RectBox structure contains two RangeBox components (range_box_x and range_box_y), each representing the X and Y dimensional constraints respectively. Each RangeBox contains left and right Range structures with low and high float8 boundaries. By initializing all boundaries to span from negative infinity to positive infinity, the function creates a constraint-free 4D space that can be progressively refined during index traversal.

This initialization is essential for the SP-GiST geometric indexing algorithm, which starts with the entire space and then applies progressive spatial constraints as it traverses the index tree structure.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - RectBox (4D rectangular box structure)
  - [palloc](../p/palloc.md) (PostgreSQL memory allocation function)
  - get_float8_infinity (PostgreSQL function to get positive infinity value)
- Called from (representative examples):
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- This is a static function, accessible only within geo_spgist.c
- Allocates memory for the RectBox using palloc, which integrates with PostgreSQL's memory management system
- Sets all 8 boundary values (4 low, 4 high) to infinity to represent unconstrained 4D space
- The RectBox represents constraints in 4D space where each 2D box is treated as a 4D point
- Used as the initial traversal value in SP-GiST consistency functions to represent the root node's coverage
- The infinity values ensure that any real geometric coordinates will fall within the initial boundaries
- Part of the geometric SP-GiST infrastructure that enables efficient spatial querying by progressive constraint refinement
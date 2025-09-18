# box_construct

## Location
src/backend/utils/adt/geo_ops.c: 518 - 550

## Overview
A static inline utility function that constructs a BOX structure from two Point structures, ensuring proper ordering of coordinates.

## Definition


## Detailed Description
The `box_construct` function creates a properly formed BOX from two arbitrary Point structures. It ensures that the resulting BOX has its coordinates arranged correctly with `high` representing the upper-right corner and `low` representing the lower-left corner, regardless of the input point order. The function compares the x and y coordinates of both input points and assigns the greater values to the `high` fields and lesser values to the `low` fields.

This function is a fundamental building block used internally by other geometric operations in PostgreSQL to create normalized BOX representations.

## Parameters / Member Variables
- `result`: Pointer to the BOX structure to be filled with the constructed box
- `pt1`: Pointer to the first Point structure
- `pt2`: Pointer to the second Point structure (can be any corner relative to pt1)

## Dependencies
- Functions called/Symbols referenced:
  - [float8_gt](../f/float8_gt.md) (floating-point comparison function)
  - [BOX](../B/BOX.md) (box data structure)
  - [Point](../P/Point.md) (point data structure)
- Called from (representative examples):
  - [points_box](../p/points_box.md) (constructs box from two points)
  - [box_mul](box_mul.md) (box multiplication operation)
  - [box_div](box_div.md) (box division operation)
  - [box_poly](box_poly.md) (converts box to polygon)

## Notes and Other Information
- The function is declared as `static inline` for performance optimization since it's frequently used internally
- Handles arbitrary point ordering - the input points don't need to represent specific corners
- Uses PostgreSQL's `float8_gt` function for proper floating-point comparison with NaN handling
- Essential for maintaining BOX invariants where high.x >= low.x and high.y >= low.y
- Located in the geometric operations module (geo_ops.c) alongside other BOX manipulation functions
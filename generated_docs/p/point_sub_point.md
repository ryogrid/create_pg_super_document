# point_sub_point

## Location
[src/backend/utils/adt/geo_ops.c:4134-4141](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4134-L4141)

## Overview
A static inline function that performs vector subtraction of two geometric points by subtracting their x and y coordinates respectively.

## Definition

```c
struct(result,
					float8_mi(pt1->x, pt2->x),
					float8_mi(pt1->y, pt2->y));
```
## Detailed Description
This function computes the difference between two Point structures by subtracting the coordinates of the second point from the first point (pt1 - pt2). The result is stored in the provided result Point structure. The function uses PostgreSQL's float8_mi function to ensure proper floating-point subtraction semantics and calls point_construct to properly initialize the result point with the computed coordinates.

## Parameters / Member Variables
- : Pointer to a Point structure where the difference will be stored
- : Pointer to the first Point operand (minuend)
- : Pointer to the second Point operand (subtrahend)

## Dependencies
- Functions called/Symbols referenced:
  - [point_construct](point_construct.md)
  - [float8_mi](../f/float8_mi.md)
  - [Point](../P/Point.md) (data type)
- Called from (representative examples):
  - [point_sub](point_sub.md)
  - [box_sub](../b/box_sub.md)
  - [path_sub_pt](path_sub_pt.md)
  - [circle_sub_pt](../c/circle_sub_pt.md)

## Notes and Other Information
- This is a static inline function for internal use within the geometric operations module
- Uses PostgreSQL's float8_mi function rather than direct C subtraction to handle special floating-point cases (NaN, infinity)
- Part of PostgreSQL's geometric data type operations infrastructure
- The function modifies the result parameter in-place rather than returning a new Point structure
- Performs the operation result = pt1 - pt2 (order matters for subtraction)
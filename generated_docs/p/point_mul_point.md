# point_mul_point

## Location
[src/backend/utils/adt/geo_ops.c:4157-4166](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4157-L4166)

## Overview
A static inline function that performs complex number multiplication of two geometric points, treating them as complex numbers where x represents the real part and y represents the imaginary part.

## Definition

```c
struct(result,
					float8_mi(float8_mul(pt1->x, pt2->x),
							  float8_mul(pt1->y, pt2->y)),
					float8_pl(float8_mul(pt1->x, pt2->y),
							  float8_mul(pt1->y, pt2->x)));
```
## Detailed Description
This function computes the complex multiplication of two Point structures by treating each point as a complex number (x + yi). The multiplication follows the complex number formula: (a + bi)(c + di) = (ac - bd) + (ad + bc)i. The result's x-coordinate becomes (pt1.x * pt2.x - pt1.y * pt2.y) and the y-coordinate becomes (pt1.x * pt2.y + pt1.y * pt2.x). The function uses PostgreSQL's float8_mul, float8_mi, and float8_pl functions to ensure proper floating-point arithmetic and calls point_construct to initialize the result.

## Parameters / Member Variables
- : Pointer to a Point structure where the complex multiplication result will be stored
- : Pointer to the first Point operand (treated as complex number pt1.x + pt1.y*i)
- : Pointer to the second Point operand (treated as complex number pt2.x + pt2.y*i)

## Dependencies
- Functions called/Symbols referenced:
  - [point_construct](point_construct.md)
  - [float8_mul](../f/float8_mul.md)
  - [float8_mi](../f/float8_mi.md)
  - [float8_pl](../f/float8_pl.md)
  - [Point](../P/Point.md) (data type)
- Called from (representative examples):
  - [point_mul](point_mul.md)
  - [box_mul](../b/box_mul.md)
  - [path_mul_pt](path_mul_pt.md)
  - [circle_mul_pt](../c/circle_mul_pt.md)

## Notes and Other Information
- This is a static inline function for internal use within the geometric operations module
- Implements complex number multiplication using the mathematical formula: (a+bi)(c+di) = (ac-bd) + (ad+bc)i
- Uses PostgreSQL's float8 arithmetic functions rather than direct C operations to handle special floating-point cases (NaN, infinity)
- Part of PostgreSQL's geometric data type operations infrastructure supporting complex geometric transformations
- The function modifies the result parameter in-place rather than returning a new Point structure
- This operation is useful for rotations and scaling transformations in 2D geometry
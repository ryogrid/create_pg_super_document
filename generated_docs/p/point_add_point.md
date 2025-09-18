# point_add_point

## Location
src/backend/utils/adt/geo_ops.c: 4111 - 4118

## Overview
A static inline function that performs vector addition of two geometric points by adding their x and y coordinates respectively.

## Definition


## Detailed Description
This function computes the sum of two Point structures by adding their corresponding x and y coordinates. The result is stored in the provided result Point structure. The function uses PostgreSQL's float8_pl function to ensure proper floating-point addition semantics and calls point_construct to properly initialize the result point with the computed coordinates.

## Parameters / Member Variables
- : Pointer to a Point structure where the sum will be stored
- : Pointer to the first Point operand 
- : Pointer to the second Point operand

## Dependencies
- Functions called/Symbols referenced:
  - [point_construct](point_construct.md)
  - [float8_pl](../f/float8_pl.md)
  - [Point](../P/Point.md) (data type)
- Called from (representative examples):
  - [point_add](point_add.md)
  - [box_add](../b/box_add.md)
  - [path_add_pt](path_add_pt.md)
  - [circle_add_pt](../c/circle_add_pt.md)
  - [poly_to_circle](poly_to_circle.md)

## Notes and Other Information
- This is a static inline function for internal use within the geometric operations module
- Uses PostgreSQL's float8_pl function rather than direct C addition to handle special floating-point cases (NaN, infinity)
- Part of PostgreSQL's geometric data type operations infrastructure
- The function modifies the result parameter in-place rather than returning a new Point structure
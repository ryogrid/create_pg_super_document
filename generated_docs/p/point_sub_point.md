# point_sub_point

## Location
src/backend/utils/adt/geo_ops.c: 4134 - 4141

## Overview
A static inline function that performs vector subtraction of two geometric points by subtracting their x and y coordinates respectively.

## Definition


## Detailed Description
This function computes the difference between two Point structures by subtracting the coordinates of the second point from the first point (pt1 - pt2). The result is stored in the provided result Point structure. The function uses PostgreSQL's float8_mi function to ensure proper floating-point subtraction semantics and calls point_construct to properly initialize the result point with the computed coordinates.

## Parameters / Member Variables
- : Pointer to a Point structure where the difference will be stored
- : Pointer to the first Point operand (minuend)
- : Pointer to the second Point operand (subtrahend)

## Dependencies
- Functions called/Symbols referenced:
  - point_construct
  - float8_mi
  - Point (data type)
- Called from (representative examples):
  - point_sub
  - box_sub
  - path_sub_pt
  - circle_sub_pt

## Notes and Other Information
- This is a static inline function for internal use within the geometric operations module
- Uses PostgreSQL's float8_mi function rather than direct C subtraction to handle special floating-point cases (NaN, infinity)
- Part of PostgreSQL's geometric data type operations infrastructure
- The function modifies the result parameter in-place rather than returning a new Point structure
- Performs the operation result = pt1 - pt2 (order matters for subtraction)
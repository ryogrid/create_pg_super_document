# rt_box_union

## Location
src/backend/access/gist/gistproc.c: 55 - 67

## Overview
Calculates the union of two BOX structures by determining the minimum bounding box that encompasses both input boxes.

## Definition


## Detailed Description
This function computes the spatial union of two boxes by finding the minimum bounding rectangle that contains both input boxes. The union operation is fundamental in spatial indexing operations, particularly in R-tree and GiST (Generalized Search Tree) implementations where bounding boxes need to be merged or expanded to accommodate new entries.

The function calculates the union by taking the maximum of the high coordinates (top-right corner) and the minimum of the low coordinates (bottom-left corner) from both input boxes. This ensures the resulting box encompasses the entire area covered by both input boxes.

## Parameters / Member Variables
- : Output parameter - pointer to the BOX structure where the union result will be stored
- : Input parameter - pointer to the first BOX structure (read-only)
- : Input parameter - pointer to the second BOX structure (read-only)

## Dependencies
- Functions called/Symbols referenced:
  - [BOX](../B/BOX.md) (data type)
  - [float8_max](../f/float8_max.md) (for calculating maximum high coordinates)
  - [float8_min](../f/float8_min.md) (for calculating minimum low coordinates)
- Called from (representative examples):
  - [box_penalty](../b/box_penalty.md)

## Notes and Other Information
- This is a static function, meaning it's only visible within the gistproc.c file
- The function operates directly on floating-point coordinates using PostgreSQL's float8 operations
- The union operation is commutative - [rt_box_union](rt_box_union.md)(n, a, b) produces the same result as rt_box_union(n, b, a)
- Part of the PostgreSQL GiST access method implementation for spatial data types
- Located in src/backend/access/gist/gistproc.c:55-67
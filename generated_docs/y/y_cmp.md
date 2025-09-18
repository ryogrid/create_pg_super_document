# y_cmp

## Location
src/backend/access/spgist/spgquadtreeproc.c: 157 - 168

## Overview
A comparison function that compares two SortedPoint structures based on their Y coordinates, used for sorting points during SP-GiST spatial index operations.

## Definition


## Detailed Description
The  function is a standard qsort-compatible comparison function that compares two  structures based on their Y coordinates. It follows the standard C library comparison function convention, returning:
- 0 if the Y coordinates are equal
- 1 if the first point's Y coordinate is greater than the second
- -1 if the first point's Y coordinate is less than the second

This function is used internally by SP-GiST (Space-partitioned Generalized Search Tree) implementations for sorting points along the Y-axis, which is essential for spatial partitioning algorithms in both k-d tree and quadtree variants.

## Parameters / Member Variables
- : Pointer to the first  structure to compare
- : Pointer to the second  structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - [SortedPoint](../S/SortedPoint.md) (structure type)
- Called from (representative examples):
  - [spg_kd_picksplit](../s/spg_kd_picksplit.md)
  - [spg_quad_picksplit](../s/spg_quad_picksplit.md)

## Notes and Other Information
- This function is defined as static, meaning it has internal linkage within the spgkdtreeproc.c file
- The function assumes that both input pointers point to valid  structures
- Used in conjunction with qsort() or similar sorting algorithms to order points by Y coordinate
- Part of the SP-GiST spatial indexing infrastructure in PostgreSQL
- Complements the  function which performs similar comparisons along the X-axis
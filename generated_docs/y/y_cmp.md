# y_cmp

## Location
[src/backend/access/spgist/spgquadtreeproc.c:157-168](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgquadtreeproc.c#L157-L168)

## Overview
A comparison function that compares two SortedPoint structures based on their Y coordinates, used for sorting points during SP-GiST spatial index operations.

## Definition

```c
static int
y_cmp(const void *a, const void *b, void *arg)
```
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

## Simplified Source

```c
static int y_cmp(const void *a, const void *b)
{
    SortedPoint *pa = (SortedPoint *) a;
    SortedPoint *pb = (SortedPoint *) b;

    // Compare y-coordinates of two points
    if (pa->p->y == pb->p->y)
        return 0;       // Equal
    return (pa->p->y > pb->p->y) ? 1 : -1;  // Greater or less than
}
```
# getSide

## Location
[src/backend/access/spgist/spgkdtreeproc.c:41-53](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/spgist/spgkdtreeproc.c#L41-L53)

## Overview
Helper function that determines the spatial relationship between a coordinate value and a test point in k-dimensional space for SP-GiST k-d tree operations.

## Definition

```c
typedef struct SortedPoint
{
	Point	   *p;
	int			i;
} SortedPoint;
```
## Detailed Description
This static function compares a given coordinate value against either the X or Y coordinate of a test point, returning an integer indicating their relative position. It serves as a fundamental building block for k-d tree spatial partitioning logic, enabling the tree to determine which side of a splitting plane a point falls on. The function supports both X and Y coordinate comparisons through the  parameter, making it suitable for alternating between dimensions in k-d tree traversal.

## Parameters / Member Variables
- : The coordinate value to compare (double precision floating-point)
- : Boolean flag indicating whether to compare against X coordinate (true) or Y coordinate (false) of the test point
- : Pointer to the test Point structure containing x and y coordinates

## Dependencies
- Functions called/Symbols referenced:
  - [Point](../P/Point.md) (structure type)
- Called from (representative examples):
  - [spg_kd_choose](../s/spg_kd_choose.md) (at src/backend/access/spgist/spgkdtreeproc.c:71)

## Notes and Other Information
- Returns 0 if coordinates are equal, 1 if coord > test coordinate, -1 if coord < test coordinate
- Used internally by the SP-GiST k-d tree implementation for spatial partitioning decisions
- The function handles exact equality comparisons, which is important for consistent tree structure
- Part of the k-d tree choose function logic that determines how to partition space during index operations
- Located in src/backend/access/spgist/spgkdtreeproc.c:41-53
# Range

## Location
[src/backend/utils/adt/geo_spgist.c:107-112](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L107-L112)

## Overview
Range is a simple structure used in PostgreSQL's geometric SP-GiST index implementation to represent a one-dimensional range with low and high boundaries.

## Definition

```c
typedef struct
{
	Range		left;
	Range		right;
} RangeBox;
```
## Detailed Description
The Range structure is a fundamental building block for 2D geometric indexing operations in PostgreSQL's SP-GiST (Space-Partitioned Generalized Search Tree) implementation. It represents a one-dimensional interval defined by two floating-point boundaries. Range structures are primarily used as components of higher-level geometric structures like RangeBox, which combines two Range instances to represent 2D rectangular regions.

This structure is specifically designed for geometric operations and spatial indexing, where efficient range comparisons and containment checks are essential for query performance.

## Parameters / Member Variables
- : The lower boundary of the range (float8/double precision)
- : The upper boundary of the range (float8/double precision)

## Dependencies
- Functions called/Symbols referenced:
  - (Used as a basic data structure, doesn't directly call other functions)
- Called from (representative examples):
  - [RangeBox](RangeBox.md) (as member variables left and right)
  - [overlap2D](../o/overlap2D.md)
  - [contain2D](../c/contain2D.md)
  - [contained2D](../c/contained2D.md)
  - [lower2D](../l/lower2D.md)
  - [overLower2D](../o/overLower2D.md)
  - [higher2D](../h/higher2D.md)
  - [overHigher2D](../o/overHigher2D.md)

## Notes and Other Information
- [Range](Range.md) is used exclusively within the geometric SP-GiST index implementation
- The structure assumes low <= high for proper range semantics
- Part of PostgreSQL's spatial indexing infrastructure for efficient 2D geometric queries
- Typically used in pairs within RangeBox structures to represent rectangular regions in 2D space
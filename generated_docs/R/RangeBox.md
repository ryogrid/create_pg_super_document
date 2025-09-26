# RangeBox

## Location
[src/backend/utils/adt/geo_spgist.c:113-118](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_spgist.c#L113-L118)

## Overview
RangeBox is a structure used in PostgreSQL's geometric SP-GiST index implementation to represent a 2D rectangular region using two one-dimensional Range structures.

## Definition

```c
typedef struct
{
	RangeBox	range_box_x;
	RangeBox	range_box_y;
} RectBox;
```
## Detailed Description
The RangeBox structure represents a 2D rectangular region by combining two Range instances. In PostgreSQL's geometric SP-GiST indexing system, RangeBox serves as an intermediate representation that bridges the gap between PostgreSQL's BOX type and the internal spatial indexing algorithms.

The structure is designed to represent points in 4D space for spatial indexing purposes, where the 'left' Range typically represents the X-axis bounds and the 'right' Range represents the Y-axis bounds. This representation facilitates efficient spatial operations like overlap detection, containment checks, and spatial partitioning within the SP-GiST index structure.

RangeBox is commonly used in conjunction with the getRangeBox() function, which converts a PostgreSQL BOX type into a RangeBox for internal processing. The structure enables more convenient access to coordinate bounds compared to the original BOX representation.

## Parameters / Member Variables
- `range_box_x`: Range structure representing the X-axis boundaries (typically low.x to high.x)
- `range_box_y`: Range structure representing the Y-axis boundaries (typically low.y to high.y)
## Dependencies
- Functions called/Symbols referenced:
  - [Range](Range.md) (as component type for left and right members)
- Called from (representative examples):
  - [getRangeBox](../g/getRangeBox.md)
  - [getQuadrant](../g/getQuadrant.md)
  - [nextRectBox](../n/nextRectBox.md)
  - [overlap2D](../o/overlap2D.md), overlap4D
  - [contain2D](../c/contain2D.md), contain4D
  - [contained2D](../c/contained2D.md), contained4D
  - [lower2D](../l/lower2D.md), overLower2D
  - [higher2D](../h/higher2D.md), overHigher2D
  - [left4D](../l/left4D.md), overLeft4D
  - [right4D](../r/right4D.md), overRight4D
  - [below4D](../b/below4D.md), overBelow4D
  - [above4D](../a/above4D.md), overAbove4D
  - [spg_box_quad_inner_consistent](../s/spg_box_quad_inner_consistent.md)

## Notes and Other Information
- Part of PostgreSQL's SP-GiST implementation for geometric data types
- Provides a more convenient representation than BOX for internal spatial indexing operations
- Used extensively in 2D and 4D spatial operations within the geometric index
- The naming convention (left/right) refers to the typical usage pattern where left represents X-coordinates and right represents Y-coordinates
- Facilitates efficient spatial partitioning and query processing in geometric indexes
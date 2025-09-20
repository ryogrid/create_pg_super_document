# box_ov

## Location
[src/backend/utils/adt/geo_ops.c:572-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L572-L582)

## Overview
Internal static function that implements the core logic for determining whether two BOX structures overlap geometrically.

## Definition

```c
static bool
box_ov(BOX *box1, BOX *box2)
```
## Detailed Description
The `box_ov` function implements the fundamental overlap detection algorithm for BOX structures. It determines overlap by checking if the boxes intersect in both the x and y dimensions using four boundary comparisons. Two boxes overlap if and only if they overlap in both dimensions simultaneously. The function uses PostgreSQL's floating-point comparison macro `FPle` (floating-point less-than-or-equal) to handle potential floating-point precision issues correctly.

The algorithm checks: box1.low.x <= box2.high.x AND box2.low.x <= box1.high.x AND box1.low.y <= box2.high.y AND box2.low.y <= box1.high.y. If all four conditions are true, the boxes overlap.

## Parameters / Member Variables
- `box1`: Pointer to the first BOX structure
- `box2`: Pointer to the second BOX structure

## Dependencies
- Functions called/Symbols referenced:
  - [FPle](../F/FPle.md) (floating-point less-than-or-equal comparison macro)
  - [BOX](../B/BOX.md) (box data structure)
- Called from (representative examples):
  - [box_overlap](box_overlap.md) (SQL-callable overlap function)
  - [box_intersect](box_intersect.md) (box intersection calculation)
  - [path_inter](../p/path_inter.md) (path intersection testing)
  - [box_interpt_lseg](box_interpt_lseg.md) (box-line segment intersection)
  - [poly_overlap_internal](../p/poly_overlap_internal.md) (polygon overlap detection)

## Notes and Other Information
- This is the core overlap detection logic used throughout PostgreSQL's geometric system
- Uses `FPle` macro instead of direct <= comparison for proper floating-point handling
- Static function, only accessible within geo_ops.c for internal use
- Implements the mathematical definition of 2D rectangle overlap
- Critical performance component for spatial indexing and geometric queries
- Used by various geometric functions requiring overlap detection (boxes, paths, polygons)
- Returns true if boxes touch at edges (inclusive overlap testing)
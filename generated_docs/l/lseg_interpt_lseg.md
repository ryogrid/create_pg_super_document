# lseg_interpt_lseg

## Location
[src/backend/utils/adt/geo_ops.c:2338-2360](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2338-L2360)

## Overview
Determines whether two line segments intersect and optionally returns the intersection point.

## Definition

```c
struct(&tmp, &l2->p[0], lseg_sl(l2));
```
## Detailed Description
This internal function calculates whether two line segments intersect. It works by first constructing a line from the second segment, then finding the intersection point between the first segment and this line. Finally, it verifies that the intersection point lies within the bounds of the second segment. If an intersection exists, the function optionally stores the intersection point in the result parameter. The function is designed to be symmetric with lseg_interpt_line() for comprehensive line-segment intersection handling.

## Parameters / Member Variables
- : Pointer to a Point structure where the intersection point will be stored (can be NULL if only intersection testing is needed)
- : First line segment
- : Second line segment
- Returns: true if segments intersect, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - [line_construct](line_construct.md) (constructs a line from two points)
  - [lseg_sl](lseg_sl.md) (calculates slope of line segment)
  - [lseg_interpt_line](lseg_interpt_line.md) (finds intersection between segment and line)
  - [lseg_contain_point](lseg_contain_point.md) (checks if point lies within segment)
- Called from:
  - [path_inter](../p/path_inter.md) (path intersection operations)
  - [lseg_intersect](lseg_intersect.md) (segment intersection testing)
  - [lseg_interpt](lseg_interpt.md) (segment intersection point calculation)
  - [lseg_closept_lseg](lseg_closept_lseg.md) (closest point between segments)
  - [box_interpt_lseg](../b/box_interpt_lseg.md) (box-segment intersection)
  - [poly_overlap_internal](../p/poly_overlap_internal.md) (polygon overlap detection)
  - [lseg_inside_poly](lseg_inside_poly.md) (segment inside polygon testing)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2338-2360
- This is a static function, meaning it's only accessible within the same source file
- The function is noted to be "almost perfectly symmetric" in design
- Uses a two-step approach: first find line intersection, then validate segment bounds
- Critical for various geometric operations involving line segment intersections in PostgreSQL's geometric data types
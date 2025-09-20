# FPge

## Location
[src/include/utils/geo_decls.h:77-81](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/geo_decls.h#L77-L81)

## Overview
FPge is an inline utility function that performs floating-point greater-than-or-equal comparison with epsilon tolerance for geometric operations in PostgreSQL.

## Definition

```c
typedef struct
{
	float8		x,
				y;
} Point;
```
## Detailed Description
The FPge function implements a floating-point comparison that accounts for numerical precision issues inherent in floating-point arithmetic. Instead of performing a direct comparison (A >= B), it adds a small epsilon value (EPSILON) to the first operand before comparison. This approach helps handle cases where floating-point rounding errors might cause mathematically equivalent values to be considered unequal. The function is widely used throughout PostgreSQL's geometric operations to ensure reliable comparisons for spatial data types.

## Parameters / Member Variables
- : First double-precision floating-point value to compare
- : Second double-precision floating-point value to compare

## Dependencies
- Functions called/Symbols referenced:
  - EPSILON (constant for floating-point tolerance)
- Called from (representative examples):
  - [gist_point_consistent_internal](../g/gist_point_consistent_internal.md)
  - [box_overright](../b/box_overright.md)
  - [box_overabove](../b/box_overabove.md)
  - [box_contain_box](../b/box_contain_box.md)
  - [box_above_eq](../b/box_above_eq.md)
  - [box_ge](../b/box_ge.md)
  - [lseg_ge](../l/lseg_ge.md)
  - [circle_overright](../c/circle_overright.md)
  - [circle_overabove](../c/circle_overabove.md)
  - [circle_ge](../c/circle_ge.md)
  - [lseg_crossing](../l/lseg_crossing.md)
  - [overlap2D](../o/overlap2D.md)
  - [contain2D](../c/contain2D.md)
  - [contained2D](../c/contained2D.md)
  - [overHigher2D](../o/overHigher2D.md)

## Notes and Other Information
This function is defined as a static inline function in src/include/utils/geo_decls.h:77-81, making it available for efficient inlining throughout the codebase. It is extensively used in geometric comparison operations, spatial indexing (GiST and SP-GiST), and various geometric predicates. The epsilon-based comparison is crucial for the reliability of PostgreSQL's geometric data types including points, boxes, line segments, and circles.
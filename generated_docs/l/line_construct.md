# line_construct

## Location
src/backend/utils/adt/geo_ops.c: 1083 - 1114

## Overview
Constructs the internal representation of a line from a point and slope. This is a utility function that converts point-slope form to the standard Ax + By + C = 0 equation form.

## Definition
```c
static inline void line_construct(LINE *result, Point *pt, float8 m)
```

## Detailed Description
The `line_construct` function is a static inline utility that fills a pre-allocated LINE structure with the coefficients A, B, and C representing the line equation Ax + By + C = 0. Given a point and a slope, it handles three cases:

1. **Vertical lines** (infinite slope): Uses the form x = C, setting A = -1, B = 0, C = pt->x
2. **Horizontal lines** (zero slope): Uses the form y = C, setting A = 0, B = -1, C = pt->y  
3. **General lines** (finite non-zero slope): Uses the form mx - y + yinter = 0, where yinter is the y-intercept calculated from the point and slope

The function includes special handling to avoid negative zero results on some platforms, ensuring consistent representation of the coefficients.

## Parameters / Member Variables
- `result`: Pre-allocated LINE structure to fill with computed coefficients
- `pt`: Point that the line passes through
- `m`: Slope of the line (can be infinite for vertical lines)

## Dependencies
- Functions called/Symbols referenced:
  - `isinf`: Tests if slope is infinite (vertical line)
  - `[float8_mi](../f/float8_mi.md)`: Floating-point subtraction
  - `[float8_mul](../f/float8_mul.md)`: Floating-point multiplication
- Called from (representative examples):
  - `[line_in](line_in.md)`: When converting two-point input format
  - `[line_construct_pp](line_construct_pp.md)`: When constructing line from two points
  - `[lseg_interpt_lseg](lseg_interpt_lseg.md)`: Line segment intersection calculations
  - `[lseg_interpt_line](lseg_interpt_line.md)`: Line segment to line intersection
  - `[line_closept_point](line_closept_point.md)`: Finding closest point on line
  - `[lseg_closept_point](lseg_closept_point.md)`: Finding closest point on line segment

## Notes and Other Information
- Part of PostgreSQL's geometric data type system in `src/backend/utils/adt/geo_ops.c`
- Static inline function for efficient internal use within geo_ops.c
- Handles all mathematical edge cases including vertical and horizontal lines
- Ensures consistent coefficient representation across platforms
- Used extensively by other geometric functions for line construction
- Line numbers: 1083-1114 in geo_ops.c
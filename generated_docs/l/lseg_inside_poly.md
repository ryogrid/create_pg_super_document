# lseg_inside_poly

## Location
[src/backend/utils/adt/geo_ops.c:3866-3937](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3866-L3937)

## Overview
lseg_inside_poly is a recursive static function that determines whether a line segment defined by two points lies completely inside a polygon.

## Definition
static bool lseg_inside_poly(Point *a, Point *b, POLYGON *poly, int start)

## Detailed Description
lseg_inside_poly implements a comprehensive algorithm to test whether a line segment (a,b) is entirely contained within a polygon. The function uses a sophisticated approach that handles multiple geometric scenarios: segments that are collinear with polygon edges, segments that intersect polygon edges, and segments that lie entirely within the polygon interior. It employs recursive calls to handle complex cases where the segment intersects polygon edges, subdividing the problem into smaller subsegments. The function includes optimizations such as a starting edge parameter to avoid redundant checks and stack depth monitoring to prevent overflow during recursion.

## Parameters / Member Variables
- : First point of the line segment to test
- : Second point of the line segment to test  
- : The polygon to test containment against
- : Starting edge index for polygon iteration (optimization parameter)

## Dependencies
- Functions called/Symbols referenced:
  - [check_stack_depth](../c/check_stack_depth.md): Prevents stack overflow during recursion
  - [lseg_contain_point](lseg_contain_point.md): Tests if a line segment contains a point
  - [touched_lseg_inside_poly](../t/touched_lseg_inside_poly.md): Handles special cases where segment touches polygon edge
  - [lseg_interpt_lseg](lseg_interpt_lseg.md): Finds intersection point between two line segments
  - [float8_pl](../f/float8_pl.md): Floating-point addition for coordinate calculations
  - [float8_div](../f/float8_div.md): Floating-point division for coordinate calculations  
  - [point_inside](../p/point_inside.md): Tests if a point lies inside the polygon
  - CHECK_FOR_INTERRUPTS: Allows query cancellation during long operations
- Called from (representative examples):
  - [touched_lseg_inside_poly](../t/touched_lseg_inside_poly.md): For recursive edge case handling
  - [poly_contain_poly](../p/poly_contain_poly.md): As part of polygon containment testing
  - [PATH_CLOSED](../P/PATH_CLOSED.md): For closed path containment testing

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:3866-3937
- Recursive function that includes stack depth checking to prevent overflow
- Handles three main geometric cases: Y-crossing (segment endpoint on polygon edge), X-crossing (segment intersects polygon edge), and no intersection
- When no intersections are found, tests the midpoint of the segment to determine interior containment
- Uses floating-point arithmetic for precise coordinate calculations
- Includes interrupt checking to allow query cancellation during complex polygon operations
- The start parameter enables optimization by allowing callers to specify which polygon edge to begin checking from
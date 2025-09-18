# touched_lseg_inside_poly

## Location
src/backend/utils/adt/geo_ops.c: 3830 - 3865

## Overview
touched_lseg_inside_poly is a specialized static function that tests whether a line segment with one endpoint touching a polygon edge is inside the polygon.

## Definition
static bool touched_lseg_inside_poly(Point *a, Point *b, LSEG *s, POLYGON *poly, int start)

## Detailed Description
touched_lseg_inside_poly handles a special geometric case when determining if a line segment is inside a polygon. This function is called when point 'a' lies on a polygon edge segment 's', but the segment (a,b) is not entirely contained within 's'. The function determines whether the segment (a,b) should be considered inside the polygon by testing various collinearity conditions and delegating to lseg_inside_poly for further analysis. The function implements sophisticated logic to handle edge cases where a segment touches a polygon boundary.

## Parameters / Member Variables
- : Point that lies on the polygon edge segment 's'
- : Second point of the line segment being tested (not on segment 's')
- : The polygon edge segment that contains point 'a'
- : The polygon being tested against
- : Starting index for polygon edge iteration in recursive calls

## Dependencies
- Functions called/Symbols referenced:
  - [point_eq_point](../p/point_eq_point.md): Tests if two points are equal
  - [lseg_contain_point](../l/lseg_contain_point.md): Tests if a line segment contains a specific point
  - [lseg_inside_poly](../l/lseg_inside_poly.md): Recursively determines if a line segment is inside the polygon
- Called from (representative examples):
  - [lseg_inside_poly](../l/lseg_inside_poly.md): Called during polygon containment testing

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:3830-3865
- This is a static helper function specifically designed for complex geometric edge cases
- The function handles four different scenarios based on which endpoint of segment 's' coincides with point 'a'
- Returns true by default in ambiguous cases, with the expectation that further validation will occur later
- Critical for accurate polygon containment testing when dealing with segments that touch polygon boundaries
- The comment indicates this function tests 'special kind of segment' where point a is on segment s but segment (a,b) is not contained by s
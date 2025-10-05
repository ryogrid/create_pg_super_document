# lseg_contain_point

## Location
[src/backend/utils/adt/geo_ops.c:3109-3116](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3109-L3116)

## Overview
Determines whether a given point lies on a line segment by using a triangle inequality detection algorithm to test for collinearity.

## Definition
```c
static bool lseg_contain_point(LSEG *lseg, Point *pt)
```

## Detailed Description
This static utility function implements geometric containment testing for points on line segments using a sophisticated collinearity detection algorithm. Unlike simple line containment, this function must verify that the point not only lies on the infinite line defined by the segment endpoints, but also falls within the bounded segment itself.

The algorithm works by applying the triangle inequality principle: if three points are collinear, then the sum of distances from the test point to each endpoint should equal the distance between the endpoints themselves. Specifically, it computes: distance(pt, p[0]) + distance(pt, p[1]) and compares this to distance(p[0], p[1]). If these values are equal (within floating-point tolerance), the point lies on the segment.

The comment notes that this algorithm behaves well even with least significant bit (lsb) residues, indicating robust floating-point behavior that was verified in 1997.

## Parameters / Member Variables
- `lseg`: Pointer to an LSEG structure containing the line segment defined by two endpoints p[0] and p[1]
- `pt`: Pointer to a Point structure containing the x and y coordinates to test for containment

## Dependencies
- Functions called/Symbols referenced:
  - [point_dt](../p/point_dt.md) (computes distance between two points)
  - [FPeq](../F/FPeq.md) (checks floating-point equality with appropriate tolerance)
- Data types used:
  - [LSEG](../L/LSEG.md) (line segment representation with two endpoint points)
  - [Point](../P/Point.md) (point representation with x, y coordinates)
- Called from (representative examples):
  - [on_ps](../o/on_ps.md) (point on line segment test)
  - [lseg_interpt_lseg](lseg_interpt_lseg.md) (line segment intersection calculations)
  - [lseg_interpt_line](lseg_interpt_line.md) (line segment-line intersection)
  - [touched_lseg_inside_poly](../t/touched_lseg_inside_poly.md) (polygon containment testing)
  - [lseg_inside_poly](lseg_inside_poly.md) (segment-polygon relationships)

## Notes and Other Information
- This is a static function, accessible only within geo_ops.c
- Uses a triangle inequality-based algorithm for robust collinearity detection
- The algorithm was specifically noted to handle floating-point precision issues well (lsb residues)
- More complex than simple line containment since it must verify the point falls within the segment bounds
- Critical for accurate geometric computations in PostgreSQL's spatial data types
- The distance-based approach provides better numerical stability than alternative parametric methods
- Part of the broader geometric operations infrastructure supporting PostgreSQL's geometric types

## Simplified Source

```c
static bool lseg_contain_point(LSEG *lseg, Point *pt) {
    // Use triangle inequality to test collinearity:
    // If point is on segment, then distance(pt,p0) + distance(pt,p1) = distance(p0,p1)
    return FPeq(point_dt(pt, &lseg->p[0]) + point_dt(pt, &lseg->p[1]),
                point_dt(&lseg->p[0], &lseg->p[1]));
}
```
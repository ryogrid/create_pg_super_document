# point_inside

## Location
[src/backend/utils/adt/geo_ops.c:5340-5396](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5340-L5396)

## Overview
Determines whether a point lies inside a polygon using the ray-casting algorithm by counting edge crossings from the point to infinity.

## Definition


## Detailed Description
The `point_inside` function implements the ray-casting (point-in-polygon) algorithm to determine if a given point lies within a polygon boundary. The algorithm works by casting a conceptual ray from the test point and counting how many times it crosses polygon edges. If the number of crossings is odd, the point is inside; if even (including zero), the point is outside.

The function translates all polygon vertices relative to the test point (making the test point the origin), then iterates through each polygon edge calling `lseg_crossing` to determine if the edge crosses the ray. Special handling is provided for the case where the point lies exactly on a polygon edge, which returns a distinct result code.

The implementation processes edges in sequence, including the closing edge from the last vertex back to the first vertex to ensure the polygon is treated as a closed shape.

## Parameters / Member Variables
- `p` (Point*): Pointer to the test point to check for containment within the polygon  
- `npts` (int): Number of vertices in the polygon (must be > 0)
- `plist` (Point*): Array of polygon vertices defining the polygon boundary

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro for debugging)
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction)
  - [lseg_crossing](../l/lseg_crossing.md) (determines if a line segment crosses the ray from origin)
  - POINT_ON_POLYGON (constant indicating point lies exactly on polygon boundary)
- Data types referenced:
  - [Point](../P/Point.md) (2D point structure with x,y coordinates)

- Called from (representative examples):
  - [poly_contain_pt](poly_contain_pt.md) (checks if polygon contains a point)
  - [pt_contained_poly](pt_contained_poly.md) (checks if point is contained in polygon)
  - [lseg_inside_poly](../l/lseg_inside_poly.md) (checks if line segment is inside polygon)
  - [on_ppath](../o/on_ppath.md) (checks if point is on a path)
  - [poly_overlap_internal](poly_overlap_internal.md) (polygon overlap detection)
  - [dist_ppoly_internal](../d/dist_ppoly_internal.md) (point-to-polygon distance calculation)

## Notes and Other Information
- This is a static (internal) function not directly exposed as a PostgreSQL function
- Return values: 0 = point outside polygon, 1 = point inside polygon, 2 = point exactly on polygon boundary
- The algorithm assumes the polygon is properly closed; the function explicitly handles the edge from the last vertex back to the first
- Uses coordinate translation to simplify calculations by making the test point the origin of the coordinate system
- The ray-casting method is robust for both convex and concave polygons
- Relies on the `lseg_crossing` function to handle the geometric details of ray-edge intersection testing
- The function assumes at least one vertex in the polygon (enforced by Assert)
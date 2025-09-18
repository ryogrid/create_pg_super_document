# dist_cpoly_internal

## Location
src/backend/utils/adt/geo_ops.c: 2571 - 2587

## Overview
Internal helper function that calculates the distance from a circle to a polygon, ensuring non-negative results.

## Definition


## Detailed Description
This static internal function computes the distance between a circle and a polygon by first calculating the distance from the circle's center point to the polygon using , then subtracting the circle's radius. If the result would be negative (indicating the circle overlaps or contains part of the polygon), it returns 0.0 to indicate they are touching or intersecting. This function implements the geometric principle that the distance from a circle to another shape is the distance from the center minus the radius, with a minimum of zero.

## Parameters / Member Variables
- : CIRCLE pointer - the source circle geometry containing center point and radius
- : POLYGON pointer - the target polygon geometry

## Dependencies
- Functions called/Symbols referenced:
  - [dist_ppoly_internal](dist_ppoly_internal.md) - calculates distance from circle center point to polygon
  - [float8_mi](../f/float8_mi.md) - performs floating-point subtraction
- Called from:
  - [dist_cpoly](dist_cpoly.md) - circle to polygon distance function
  - [dist_polyc](dist_polyc.md) - polygon to circle distance function

## Notes and Other Information
- Located at src/backend/utils/adt/geo_ops.c:2571-2587
- Static function, not directly accessible outside this compilation unit
- Implements non-negative distance semantics (returns 0.0 for overlapping geometries)
- Shared implementation for both circle-to-polygon and polygon-to-circle distance calculations
- Uses point-to-polygon distance as the foundation for circle-to-polygon distance
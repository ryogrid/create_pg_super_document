# poly_contain_pt

## Location
src/backend/utils/adt/geo_ops.c: 4008 - 4016

## Overview
PostgreSQL function that determines whether a polygon contains a specific point by using the point-in-polygon algorithm.

## Definition


## Detailed Description
This function tests whether a given point lies inside a polygon. It extracts a polygon and a point from the function arguments, then delegates to the point_inside function to perform the actual geometric computation. The point_inside function implements a point-in-polygon algorithm that works with the polygon's vertex array (poly->p) and vertex count (poly->npts). The result is a boolean value indicating whether the point is contained within the polygon boundaries.

## Parameters / Member Variables
- : PostgreSQL function call context containing two arguments
  - Argument 0: The polygon (POLYGON) to test containment within
  - Argument 1: The point (Point) to test for containment

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (extract polygon argument)
  - PG_GETARG_POINT_P (extract point argument)
  - point_inside (perform point-in-polygon test)
  - PG_RETURN_BOOL (return boolean result)
- Called from (representative examples):
  - gist_point_consistent (at src/backend/access/gist/gistproc.c:1411)

## Notes and Other Information
- The function is part of PostgreSQL's geometric data type operations in src/backend/utils/adt/geo_ops.c
- Used by GiST (Generalized Search Tree) index operations for spatial queries
- The point_inside function handles the mathematical complexity of determining point containment within polygon boundaries
- Located at src/backend/utils/adt/geo_ops.c:4008-4016
- No explicit memory management is needed as the function doesn't create copies of the input data
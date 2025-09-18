# pt_contained_circle

## Location
src/backend/utils/adt/geo_ops.c: 5094 - 5108

## Overview
Tests whether a given point is contained within or on the boundary of a circle, with the point as the first parameter.

## Definition


## Detailed Description
This function determines if a point lies within a circle by calculating the distance from the point to the circle's center and comparing it to the circle's radius. It performs the same logic as circle_contain_pt but with reversed parameter order (point first, then circle). If the distance is less than or equal to the radius, the point is considered contained within the circle.

## Parameters / Member Variables
- Point (PG_GETARG_POINT_P(0)): Input point to test for containment
- Circle (PG_GETARG_CIRCLE_P(1)): Input circle structure containing center point and radius

## Dependencies
- Functions called/Symbols referenced:
  - Point (type definition)
  - CIRCLE (type definition)
  - PG_GETARG_POINT_P (parameter extraction macro)
  - PG_GETARG_CIRCLE_P (parameter extraction macro)
  - point_dt (distance between two points)
  - PG_RETURN_BOOL (boolean return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Functionally equivalent to circle_contain_pt but with reversed parameter order
- Returns true if the point is exactly on the circle's boundary (distance equals radius)
- Provides alternative syntax for point-in-circle containment testing
- Part of PostgreSQL's geometric containment operations
- Located in src/backend/utils/adt/geo_ops.c:5094-5108
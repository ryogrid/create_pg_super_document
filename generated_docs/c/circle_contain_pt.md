# circle_contain_pt

## Location
[src/backend/utils/adt/geo_ops.c:5082-5093](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5082-L5093)

## Overview
Tests whether a given point is contained within or on the boundary of a circle.

## Definition


## Detailed Description
This function determines if a point lies within a circle by calculating the distance from the point to the circle's center and comparing it to the circle's radius. If the distance is less than or equal to the radius, the point is considered contained within the circle.

## Parameters / Member Variables
- Circle (PG_GETARG_CIRCLE_P(0)): Input circle structure containing center point and radius
- Point (PG_GETARG_POINT_P(1)): Input point to test for containment

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - [Point](../P/Point.md) (type definition)
  - PG_GETARG_CIRCLE_P (parameter extraction macro)
  - PG_GETARG_POINT_P (parameter extraction macro)
  - [point_dt](../p/point_dt.md) (distance between two points)
  - PG_RETURN_BOOL (boolean return value macro)
- Called from (representative examples):
  - [gist_point_consistent](../g/gist_point_consistent.md) (in src/backend/access/gist/gistproc.c:1438)

## Notes and Other Information
- Returns true if the point is exactly on the circle's boundary (distance equals radius)
- Used in GiST index operations for spatial queries
- Part of PostgreSQL's geometric containment operations
- Located in src/backend/utils/adt/geo_ops.c:5082-5093
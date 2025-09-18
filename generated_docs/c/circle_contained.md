# circle_contained

## Location
src/backend/utils/adt/geo_ops.c: 4826 - 4837

## Overview
Tests whether the first circle is completely contained within the second circle, implementing the PostgreSQL "<@" geometric operator for circles.

## Definition
Datum circle_contained(PG_FUNCTION_ARGS)

## Detailed Description
The circle_contained function implements the "contained by" geometric operator for circle types in PostgreSQL. It determines if the first circle is completely contained within the second circle by checking if the distance between the two centers plus the radius of the first circle is less than or equal to the radius of the second circle.

The containment test uses the mathematical principle that for circle1 to be contained in circle2, the distance from circle1s center to circle2s center plus circle1s radius must not exceed circle2s radius. This ensures that all points of circle1 lie within or on the boundary of circle2.

## Parameters / Member Variables
- circle1 (PG_GETARG_CIRCLE_P(0)): Pointer to the first CIRCLE structure (the potentially contained circle)
- circle2 (PG_GETARG_CIRCLE_P(1)): Pointer to the second CIRCLE structure (the potentially containing circle)

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - PG_GETARG_CIRCLE_P (argument extraction macro)
  - [point_dt](../p/point_dt.md) (distance calculation between two points)
  - [FPle](../F/FPle.md) (floating-point less-than-or-equal comparison)
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction)
  - PG_RETURN_BOOL (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:4826-4837
- Part of PostgreSQLs geometric data type operations
- Uses point_dt to calculate the Euclidean distance between circle centers
- The containment condition is: distance(center1, center2) + radius1 <= radius2
- Returns true if circle1 is completely contained within circle2, false otherwise
- This operator is used for spatial queries involving circle containment relationships
- Handles edge cases where circles are tangent (touching at exactly one point)
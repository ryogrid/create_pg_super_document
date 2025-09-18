# circle_below

## Location
src/backend/utils/adt/geo_ops.c: 4851 - 4862

## Overview
Tests whether the first circle is positioned strictly below the second circle, implementing the PostgreSQL "<<|" geometric operator for circles.

## Definition
Datum circle_below(PG_FUNCTION_ARGS)

## Detailed Description
The circle_below function implements the "strictly below" geometric operator for circle types in PostgreSQL. It determines if the first circle is positioned entirely below the second circle by comparing the topmost point of the first circle with the bottommost point of the second circle. The function returns true if the highest point of circle1 (center.y + radius) is strictly less than the lowest point of circle2 (center.y - radius).

This ensures that there is no vertical overlap between the two circles - circle1 must be completely below circle2 with some gap between them. The comparison uses floating-point arithmetic with appropriate precision handling.

## Parameters / Member Variables
- circle1 (PG_GETARG_CIRCLE_P(0)): Pointer to the first CIRCLE structure (the potentially lower circle)
- circle2 (PG_GETARG_CIRCLE_P(1)): Pointer to the second CIRCLE structure (the potentially upper circle)

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - PG_GETARG_CIRCLE_P (argument extraction macro)
  - float8_pl (floating-point addition)
  - float8_mi (floating-point subtraction)
  - FPlt (floating-point less-than comparison)
  - PG_RETURN_BOOL (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:4851-4862
- Part of PostgreSQLs geometric data type operations
- The comparison condition is: (circle1.center.y + circle1.radius) < (circle2.center.y - circle2.radius)
- Uses strict inequality, so circles that are just touching are not considered "below"
- Returns true only when there is a clear vertical separation between the circles
- This operator is used for vertical spatial relationship queries
- Complements other positional operators like circle_above for comprehensive spatial indexing
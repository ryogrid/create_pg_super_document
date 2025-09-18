# circle_above

## Location
[src/backend/utils/adt/geo_ops.c:4863-4875](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4863-L4875)

## Overview
Tests whether the first circle is positioned strictly above the second circle, implementing the PostgreSQL "|>>" geometric operator for circles.

## Definition
Datum circle_above(PG_FUNCTION_ARGS)

## Detailed Description
The circle_above function implements the "strictly above" geometric operator for circle types in PostgreSQL. It determines if the first circle is positioned entirely above the second circle by comparing the bottommost point of the first circle with the topmost point of the second circle. The function returns true if the lowest point of circle1 (center.y - radius) is strictly greater than the highest point of circle2 (center.y + radius).

This ensures that there is no vertical overlap between the two circles - circle1 must be completely above circle2 with some gap between them. This is the complement of the circle_below operator. The comparison uses floating-point arithmetic with appropriate precision handling.

## Parameters / Member Variables
- circle1 (PG_GETARG_CIRCLE_P(0)): Pointer to the first CIRCLE structure (the potentially upper circle)
- circle2 (PG_GETARG_CIRCLE_P(1)): Pointer to the second CIRCLE structure (the potentially lower circle)

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - PG_GETARG_CIRCLE_P (argument extraction macro)
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction)
  - [float8_pl](../f/float8_pl.md) (floating-point addition)
  - [FPgt](../F/FPgt.md) (floating-point greater-than comparison)
  - PG_RETURN_BOOL (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:4863-4875
- Part of PostgreSQLs geometric data type operations
- The comparison condition is: (circle1.center.y - circle1.radius) > (circle2.center.y + circle2.radius)
- Uses strict inequality, so circles that are just touching are not considered "above"
- Returns true only when there is a clear vertical separation between the circles
- This operator is the complement of circle_below for comprehensive vertical positioning queries
- Used for vertical spatial relationship queries and spatial indexing operations
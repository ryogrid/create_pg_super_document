# circle_contain

## Location
[src/backend/utils/adt/geo_ops.c:4838-4850](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4838-L4850)

## Overview
Tests whether the first circle completely contains the second circle, implementing the PostgreSQL "@>" geometric operator for circles.

## Definition
Datum circle_contain(PG_FUNCTION_ARGS)

## Detailed Description
The circle_contain function implements the "contains" geometric operator for circle types in PostgreSQL. It determines if the first circle completely contains the second circle by checking if the distance between the two centers plus the radius of the second circle is less than or equal to the radius of the first circle.

This is the inverse operation of circle_contained. The containment test uses the mathematical principle that for circle1 to contain circle2, the distance from circle1s center to circle2s center plus circle2s radius must not exceed circle1s radius. This ensures that all points of circle2 lie within or on the boundary of circle1.

## Parameters / Member Variables
- circle1 (PG_GETARG_CIRCLE_P(0)): Pointer to the first CIRCLE structure (the potentially containing circle)
- circle2 (PG_GETARG_CIRCLE_P(1)): Pointer to the second CIRCLE structure (the potentially contained circle)

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
- Located in src/backend/utils/adt/geo_ops.c:4838-4850
- Part of PostgreSQLs geometric data type operations
- Uses point_dt to calculate the Euclidean distance between circle centers
- The containment condition is: distance(center1, center2) + radius2 <= radius1
- Returns true if circle1 completely contains circle2, false otherwise
- This operator is the complement of the circle_contained operator
- Used for spatial queries involving circle containment relationships
- Handles edge cases where circles are tangent (touching at exactly one point)

## Simplified Source

```c
Datum circle_contain(PG_FUNCTION_ARGS) {
    CIRCLE *circle1 = PG_GETARG_CIRCLE_P(0);
    CIRCLE *circle2 = PG_GETARG_CIRCLE_P(1);

    // Check if circle1 contains circle2:
    // distance(centers) + radius2 <= radius1
    PG_RETURN_BOOL(FPle(point_dt(&circle1->center, &circle2->center),
                        circle1->radius - circle2->radius));
}
```
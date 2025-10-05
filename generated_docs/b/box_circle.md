# box_circle

## Location
[src/backend/utils/adt/geo_ops.c:5208-5224](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5208-L5224)

## Overview
Converts a rectangular box to its circumscribed circle (the smallest circle that contains the entire box).

## Definition

```c
Datum
box_circle(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs a geometric conversion from a BOX (rectangular) data type to a CIRCLE. It creates the circumscribed circle of the input box - the smallest circle that completely contains the rectangle. The function calculates the center of the circle as the midpoint of the box, and sets the radius as the distance from the center to any corner of the box (specifically the high corner). This ensures that all corners of the original box lie on or within the resulting circle.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Input BOX structure accessed via 

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts BOX argument from function args
  -  - PostgreSQL memory allocation function
  -  - PostgreSQL safe floating-point addition
  -  - PostgreSQL safe floating-point division
  -  - Calculates distance between two points
  -  - Returns CIRCLE result to PostgreSQL
- Data types used:
  -  - Input box structure
  -  - Output circle structure
  -  - PostgreSQL function return type
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The center calculation uses the midpoint formula: (high + low) / 2 for both x and y coordinates
- The radius is calculated as the distance from center to the high corner, ensuring the circle circumscribes the box
- Uses PostgreSQL's safe arithmetic functions to prevent numerical errors
- This creates the circumscribed circle (not the inscribed circle that would fit inside the box)
- Can be called from SQL to convert box geometric types to circle types
- Memory allocation is handled by PostgreSQL's memory context system
- The resulting circle will have its diameter equal to the diagonal length of the original box

## Simplified Source

```c
Datum box_circle(PG_FUNCTION_ARGS) {
    BOX *box = PG_GETARG_BOX_P(0);
    CIRCLE *circle = (CIRCLE *) palloc(sizeof(CIRCLE));

    // Calculate center as midpoint of box
    circle->center.x = (box->high.x + box->low.x) / 2.0;
    circle->center.y = (box->high.y + box->low.y) / 2.0;

    // Set radius as distance from center to corner
    circle->radius = point_dt(&circle->center, &box->high);

    PG_RETURN_CIRCLE_P(circle);
}
```
# cr_circle

## Location
[src/backend/utils/adt/geo_ops.c:5170-5185](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L5170-L5185)

## Overview
Creates a CIRCLE geometric object from a center point and radius value.

## Definition

```c
Datum
cr_circle(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a conversion operator that constructs a PostgreSQL CIRCLE geometric data type from two input parameters: a center point and a radius. It allocates memory for a new CIRCLE structure, copies the x and y coordinates from the input Point to the circle's center, and sets the radius. This function serves as a constructor for creating circle objects in PostgreSQL's geometric type system.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - : Input Point structure accessed via  - the center coordinates of the circle
  - : Input float8 value accessed via  - the radius of the circle

## Dependencies
- Functions called/Symbols referenced:
  -  - Extracts Point argument from function args
  -  - Extracts float8 argument from function args
  -  - PostgreSQL memory allocation function
  -  - Returns CIRCLE result to PostgreSQL
- Data types used:
  -  - Input center point structure
  -  - Input radius value type
  -  - Output circle structure
  -  - PostgreSQL function return type
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This is a conversion operator function used to create circles from component parts
- The function allocates memory using , which is managed by PostgreSQL's memory context system
- Can be called from SQL using syntax like 
- The function performs a simple data structure conversion without validation of the radius value
- Part of PostgreSQL's geometric operators section for type conversions

## Simplified Source

```c
Datum cr_circle(PG_FUNCTION_ARGS) {
    Point *center = PG_GETARG_POINT_P(0);
    float8 radius = PG_GETARG_FLOAT8(1);
    CIRCLE *result;

    // Allocate memory for the result circle
    result = (CIRCLE *) palloc(sizeof(CIRCLE));

    // Copy center coordinates and set radius
    result->center.x = center->x;
    result->center.y = center->y;
    result->radius = radius;

    PG_RETURN_CIRCLE_P(result);
}
```

This function constructs a circle from a center point and radius. It allocates a new CIRCLE structure, copies the center coordinates from the input point, assigns the radius value, and returns the constructed circle.
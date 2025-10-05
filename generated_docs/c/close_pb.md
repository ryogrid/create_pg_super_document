# close_pb

## Location
[src/backend/utils/adt/geo_ops.c:2933-2959](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2933-L2959)

## Overview
The `close_pb` function calculates the closest point on a box to a given point, returning the coordinates of that closest point.

## Definition
```c
Datum close_pb(PG_FUNCTION_ARGS)
```

## Detailed Description
This function is a PostgreSQL built-in function that computes the point on the boundary or interior of a box that is closest to a given input point. It takes a point and a box as input parameters and returns a new point representing the closest point on the box. The function handles potential NaN (Not a Number) cases and returns NULL if the calculation results in NaN values, ensuring robust geometric computation.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - Argument 0: `Point *pt` - The reference point to find the closest point to
  - Argument 1: `BOX *box` - The box geometry to find the closest point on

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINT_P` - Extracts point argument from function parameters
  - `PG_GETARG_BOX_P` - Extracts box argument from function parameters
  - [Point](../P/Point.md) - [Point](../P/Point.md) data type definition
  - [BOX](../B/BOX.md) - Box data type definition
  - [palloc](../p/palloc.md) - PostgreSQL memory allocation function
  - [box_closept_point](../b/box_closept_point.md) - Core geometric function that performs the closest point calculation
  - `isnan` - Standard C library function to check for NaN values
  - `PG_RETURN_POINT_P` - Returns point result from PostgreSQL function
- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL function interface)

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations
- The function properly handles edge cases by checking for NaN results and returning NULL when appropriate
- Memory for the result point is allocated using PostgreSQL's memory management system (`palloc`)
- The actual geometric computation is delegated to the `box_closept_point` helper function
- Located in the geometric operations module (`geo_ops.c`) at lines 2933-2959

## Simplified Source

```c
Datum close_pb(PG_FUNCTION_ARGS) {
    Point *pt = PG_GETARG_POINT_P(0);
    BOX *box = PG_GETARG_BOX_P(1);
    Point *result;

    // Allocate memory for result point
    result = (Point *) palloc(sizeof(Point));

    // Calculate closest point on box to point
    if (isnan(box_closept_point(result, box, pt)))
        PG_RETURN_NULL();

    PG_RETURN_POINT_P(result);
}
```
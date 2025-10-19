# box_center

## Location
[src/backend/utils/adt/geo_ops.c:849-862](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L849-L862)

## Overview
Returns the center point of a geometric box as a Point data type in PostgreSQL.

## Definition

```c
Datum
box_center(PG_FUNCTION_ARGS)
```
## Detailed Description
The `box_center` function calculates and returns the center point of a BOX geometric data type. It extracts the BOX argument from the function call, allocates memory for a new Point structure, and uses the `box_cn` helper function to compute the center coordinates. The resulting Point is returned to the caller.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that contains:
  - BOX pointer: The box geometry whose center point is to be calculated

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_BOX_P`: PostgreSQL macro to extract BOX pointer from function arguments
  - [Point](../P/Point.md): PostgreSQL geometric point data type
  - [palloc](../p/palloc.md): PostgreSQL memory allocation function
  - [box_cn](box_cn.md): Helper function that calculates the center coordinates of a box
  - `PG_RETURN_POINT_P`: PostgreSQL macro to return a Point pointer
- Called from (representative examples):
  - No direct references found in the analyzed codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operations located in `src/backend/utils/adt/geo_ops.c:849-862`
- Memory for the result Point is dynamically allocated using `palloc`
- Returns a Point data type containing the x,y coordinates of the box's center
- Part of the geometric operations suite for the BOX data type in PostgreSQL
- The actual center calculation is delegated to the `box_cn` helper function

## Simplified Source

```c
Datum box_center(PG_FUNCTION_ARGS) {
    // Extract the box argument
    BOX *box = PG_GETARG_BOX_P(0);

    // Allocate memory for the result point
    Point *result = (Point *) palloc(sizeof(Point));

    // Calculate center coordinates using helper function
    box_cn(result, box);

    return PG_RETURN_POINT_P(result);
}
```
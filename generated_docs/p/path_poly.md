# path_poly

## Location
[src/backend/utils/adt/geo_ops.c:4452-4493](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4452-L4493)

## Overview
Converts a closed geometric path to a polygon by copying the path's points and creating a bounding box.

## Definition

```c
Datum
path_poly(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function converts a closed PATH geometric type to a POLYGON type. It validates that the input path is closed (not an open path), then allocates memory for a new polygon structure and copies all points from the path to the polygon. After copying the points, it computes and sets the bounding box for the polygon using . This function is part of PostgreSQL's geometric data type conversion system.

## Parameters / Member Variables
- Input: A PATH pointer obtained via  - the path to be converted
- Returns: A POLYGON pointer via  - the resulting polygon

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P (macro to extract PATH argument)
  - [palloc](palloc.md) (memory allocation)
  - SET_VARSIZE (macro to set variable-length type size)
  - [make_bound_box](../m/make_bound_box.md) (computes bounding box for polygon)
  - PG_RETURN_POLYGON_P (macro to return POLYGON result)
  - ereport (error reporting)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- The function enforces that only closed paths can be converted to polygons, raising an error for open paths
- Memory allocation is carefully calculated using 
- The function preserves the exact coordinates of all points from the source path
- A bounding box is automatically computed for the resulting polygon to optimize geometric operations
- Located in src/backend/utils/adt/geo_ops.c:4452-4493

## Simplified Source

```c
Datum
path_poly(PG_FUNCTION_ARGS)
{
    PATH *path = PG_GETARG_PATH_P(0);
    POLYGON *poly;
    int size, i;

    // Validate that path is closed (required for polygon conversion)
    if (!path->closed)
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("open path cannot be converted to polygon")));

    // Allocate memory for polygon structure with all points
    size = offsetof(POLYGON, p) + sizeof(poly->p[0]) * path->npts;
    poly = (POLYGON *) palloc(size);

    // Set polygon properties
    SET_VARSIZE(poly, size);
    poly->npts = path->npts;

    // Copy all points from path to polygon
    for (i = 0; i < path->npts; i++) {
        poly->p[i].x = path->p[i].x;
        poly->p[i].y = path->p[i].y;
    }

    // Calculate and set bounding box for polygon
    make_bound_box(poly);

    PG_RETURN_POLYGON_P(poly);
}
```
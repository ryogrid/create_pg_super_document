# poly_path

## Location
[src/backend/utils/adt/geo_ops.c:4564-4610](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4564-L4610)

## Overview
Converts a POLYGON geometric type to a PATH geometric type, preserving all the vertices and maintaining the closed path property.

## Definition

```c
Datum
poly_path(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function performs a geometric type conversion from a PostgreSQL POLYGON to a PATH. It creates a new PATH structure that contains the same vertices as the input polygon, with the path marked as closed (since polygons are inherently closed geometric shapes). The function allocates memory for the new PATH structure and copies all coordinate points from the polygon to the path while preserving their order and positions.

The conversion is straightforward as both POLYGON and PATH structures store points in similar ways - the main difference is that PATH can be either open or closed, while POLYGON is always closed.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to the input polygon parameter
## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (retrieves the input polygon argument)
  - [palloc](palloc.md) (allocates memory for the new path)
  - SET_VARSIZE (sets the variable size header for the path)
  - PG_RETURN_PATH_P (returns the path result)
- Types referenced:
  - [POLYGON](../P/POLYGON.md) (input geometric type)
  - [PATH](../P/PATH.md) (output geometric type)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- The function ensures memory safety by calculating the exact size needed for the PATH structure based on the number of points
- The conversion never overflows as the new PATH size is smaller than or equal to the original POLYGON size
- The resulting path is always marked as closed (path->closed = true) since polygons are closed by definition
- All coordinate precision is preserved during the conversion
- The dummy field is explicitly set to 0 to prevent instability in unused padding bytes
- Located in src/backend/utils/adt/geo_ops.c:4564-4610
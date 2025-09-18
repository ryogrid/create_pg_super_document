# poly_npoints

## Location
src/backend/utils/adt/geo_ops.c: 4494 - 4502

## Overview
Returns the number of points (vertices) in a polygon geometric type.

## Definition
```c
Datum poly_npoints(PG_FUNCTION_ARGS)
```

## Detailed Description
The `poly_npoints` function is a simple accessor function that returns the number of points (vertices) contained in a POLYGON geometric type. It extracts the polygon from the function arguments and directly returns the `npts` field, which stores the vertex count. This function provides a way for SQL queries to determine the complexity of a polygon by counting its vertices.

## Parameters / Member Variables
- Input: A POLYGON pointer obtained via `PG_GETARG_POLYGON_P(0)` - the polygon to query
- Returns: An int32 via `PG_RETURN_INT32()` - the number of points in the polygon

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (macro to extract POLYGON argument)
  - PG_RETURN_INT32 (macro to return 32-bit integer result)
- Called from (representative examples):
  - No direct callers found (likely called via SQL function interface)

## Notes and Other Information
- This is a very lightweight function that performs no validation or complex computation
- The function directly accesses the `npts` field of the POLYGON structure
- Part of PostgreSQL's polygon utility functions for geometric operations
- Located in src/backend/utils/adt/geo_ops.c:4494-4502
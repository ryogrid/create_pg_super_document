# construct_point

## Location
[src/backend/utils/adt/geo_ops.c:4096-4110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4096-L4110)

## Overview
PostgreSQL function that creates a new Point structure from two floating-point coordinates (x, y) and returns it as a PostgreSQL geometric data type.

## Definition

```c
Datum
construct_point(PG_FUNCTION_ARGS)
```
## Detailed Description
This function constructs a 2D point from two floating-point coordinate values. It extracts x and y coordinates from the function arguments, allocates memory for a new Point structure using palloc, and initializes the point using the point_construct helper function. The function is part of PostgreSQL's geometric data type system and provides a way to create Point objects from coordinate values. The newly created point is returned as a PostgreSQL Datum that can be used by other geometric functions or stored in the database.

## Parameters / Member Variables
- : PostgreSQL function call context containing two arguments
  - Argument 0: X coordinate (float8) of the point
  - Argument 1: Y coordinate (float8) of the point

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_FLOAT8 (extract floating-point coordinate arguments)
  - [palloc](../p/palloc.md) (allocate memory for Point structure)
  - [point_construct](../p/point_construct.md) (initialize Point structure with coordinates)
  - PG_RETURN_POINT_P (return Point as PostgreSQL Datum)
- Called from (representative examples):
  - No direct references found in the codebase (likely used via SQL point construction functions)

## Notes and Other Information
- The function is part of PostgreSQL's geometric data type operations in src/backend/utils/adt/geo_ops.c
- Marks the beginning of the "Routines for 2D points" section in the source file
- Uses PostgreSQL's memory management system (palloc) for proper memory allocation
- The point_construct function handles the actual initialization of coordinate values
- Located at src/backend/utils/adt/geo_ops.c:4096-4110
- Memory allocated with palloc will be automatically freed when the current memory context is reset
- Provides the foundation for creating Point objects that can be used by other geometric operations
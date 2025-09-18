# line_interpt

## Location
src/backend/utils/adt/geo_ops.c: 1286 - 1313

## Overview
Computes the intersection point of two lines and returns it as a Point, or NULL if the lines do not intersect.

## Definition


## Detailed Description
This function calculates the intersection point of two LINE objects. It serves as a PostgreSQL function wrapper that handles the SQL function interface for line intersection operations. The function allocates memory for a result Point and delegates the actual intersection calculation to the  helper function. If the lines are parallel or do not intersect, the function returns NULL.

## Parameters / Member Variables
- : PostgreSQL function argument macro that provides access to:
  - Argument 0: First LINE object (l1)
  - Argument 1: Second LINE object (l2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LINE_P (retrieves LINE arguments)
  - [palloc](../p/palloc.md) (memory allocation)
  - [line_interpt_line](line_interpt_line.md) (performs intersection calculation)
  - PG_RETURN_NULL (returns NULL result)
  - PG_RETURN_POINT_P (returns Point result)
- Types used:
  - LINE (geometric line type)
  - [Point](../P/Point.md) (geometric point type)
  - Datum (PostgreSQL data type)
- Called from:
  - No direct references found (likely called via SQL function interface)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1286-1313
- Part of PostgreSQL's geometric data type operations
- Returns NULL when lines are parallel or identical
- Memory for the result Point is allocated using palloc
- The actual intersection logic is implemented in the line_interpt_line helper function
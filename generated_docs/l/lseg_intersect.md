# lseg_intersect

## Location
src/backend/utils/adt/geo_ops.c: 2188 - 2197

## Overview
Determines whether two line segments intersect and returns a boolean result as a PostgreSQL function.

## Definition
```c
Datum lseg_intersect(PG_FUNCTION_ARGS)
```

## Detailed Description
The `lseg_intersect` function is a PostgreSQL built-in function that tests whether two line segments intersect. It extracts two line segments from the function arguments and uses the `lseg_interpt_lseg` helper function to determine if they intersect. The function passes NULL as the first parameter to `lseg_interpt_lseg`, indicating that it only wants to know if an intersection exists, not the actual intersection point. The result is returned as a PostgreSQL boolean value that can be used in SQL queries.

## Parameters / Member Variables
- Function follows PostgreSQL's PG_FUNCTION_ARGS convention:
  - First argument: Line segment l1 (retrieved using PG_GETARG_LSEG_P(0))
  - Second argument: Line segment l2 (retrieved using PG_GETARG_LSEG_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSEG_P: PostgreSQL macro to extract line segment arguments
  - [lseg_interpt_lseg](lseg_interpt_lseg.md): Internal function that computes intersection of two line segments
  - PG_RETURN_BOOL: PostgreSQL macro to return boolean result
  - [LSEG](../L/LSEG.md): Line segment data structure type

- Called from (representative examples):
  - [interpt_pp](../i/interpt_pp.md): Used in regression tests
  - This function is typically called from SQL queries rather than C code

## Notes and Other Information
- This is a PostgreSQL built-in function accessible from SQL as lseg_intersect()
- Only returns whether intersection exists (true/false), not the intersection point itself
- The underlying `lseg_interpt_lseg` function uses sophisticated geometric algorithms to handle edge cases
- Part of PostgreSQL's geometric data type support for spatial queries
- The intersection test considers both line segments as finite, not as infinite lines
- Useful for spatial queries and geometric analysis in PostgreSQL databases
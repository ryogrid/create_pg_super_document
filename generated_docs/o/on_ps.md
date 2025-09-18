# on_ps

## Location
[src/backend/utils/adt/geo_ops.c:3117-3129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3117-L3129)

## Overview
PostgreSQL function that tests whether a point lies on a line segment, serving as the SQL-callable interface for point-segment containment testing.

## Definition
```c
Datum on_ps(PG_FUNCTION_ARGS)
```

## Detailed Description
This function implements the PostgreSQL SQL operator that determines if a point is contained within (lies on) a line segment. It acts as a wrapper around the internal `lseg_contain_point` function, providing the necessary interface for PostgreSQL's function call convention. The function extracts the point and line segment arguments from the PostgreSQL function call arguments and delegates the actual geometric computation to the `lseg_contain_point` utility function.

This function is typically invoked through SQL queries using geometric operators that test point-segment relationships, supporting PostgreSQL's comprehensive geometric data type system. Unlike point-line containment, this function verifies that the point lies within the bounded segment rather than just on the infinite line.

## Parameters / Member Variables
- Uses `PG_FUNCTION_ARGS` macro to access function arguments:
  - Argument 0: Point structure (extracted via `PG_GETARG_POINT_P(0)`)
  - Argument 1: LSEG structure (extracted via `PG_GETARG_LSEG_P(1)`)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (extracts Point argument from PostgreSQL function args)
  - PG_GETARG_LSEG_P (extracts LSEG argument from PostgreSQL function args)
  - [lseg_contain_point](../l/lseg_contain_point.md) (performs the actual geometric containment test)
  - PG_RETURN_BOOL (returns boolean result to PostgreSQL)
- Data types used:
  - [Point](../P/Point.md) (geometric point type)
  - [LSEG](../L/LSEG.md) (geometric line segment type)
  - Datum (PostgreSQL's generic data type for function returns)
- Called from:
  - SQL queries using geometric containment operators
  - PostgreSQL's function dispatch system

## Notes and Other Information
- This is a PostgreSQL-callable function following the PG_FUNCTION_ARGS convention
- Returns a boolean Datum indicating whether the point lies on the line segment
- Part of PostgreSQL's geometric operator infrastructure
- The function name follows PostgreSQL's geometric operator naming pattern (on_ps = 'on point-segment')
- Provides the SQL interface for containment operators in geometric contexts involving line segments
- More restrictive than point-line containment since it requires the point to be within segment bounds
- Currently shows no direct references in the codebase, suggesting it may be called through PostgreSQL's dynamic function dispatch system or SQL operator resolution
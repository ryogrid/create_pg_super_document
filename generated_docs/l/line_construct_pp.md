# line_construct_pp

## Location
[src/backend/utils/adt/geo_ops.c:1115-1136](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1115-L1136)

## Overview
Constructs a LINE object from two distinct Point objects in PostgreSQL's geometric data type system.

## Definition

```c
Datum
line_construct_pp(PG_FUNCTION_ARGS)
```
## Detailed Description
This function creates a LINE geometric object by taking two Point arguments and constructing a line that passes through both points. The function performs validation to ensure the two points are distinct (not equal), as a line cannot be constructed from identical points. It uses the internal  helper function along with  to calculate the slope between the points and construct the final LINE object.

## Parameters / Member Variables
- : PostgreSQL function argument macro that expands to access two Point parameters:
  - First point (pt1): Starting point for line construction
  - Second point (pt2): Ending point for line construction

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P: Extracts Point arguments from function call
  - [point_eq_point](../p/point_eq_point.md): Validates that the two points are distinct
  - [point_sl](../p/point_sl.md): Calculates slope between two points
  - [line_construct](line_construct.md): Internal helper to construct LINE from point and slope
  - PG_RETURN_LINE_P: Returns the constructed LINE object
  - [palloc](../p/palloc.md): Memory allocation for LINE structure
- Called from (representative examples):
  - No direct callers found (likely called via PostgreSQL's function dispatch system)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:1115-1136
- Throws an error with ERRCODE_INVALID_PARAMETER_VALUE if the two input points are identical
- Part of PostgreSQL's geometric data type operations system
- The function follows PostgreSQL's V1 calling convention using PG_FUNCTION_ARGS macro

## Simplified Source

```c
Datum
line_construct_pp(PG_FUNCTION_ARGS)
{
    Point *pt1 = PG_GETARG_POINT_P(0);
    Point *pt2 = PG_GETARG_POINT_P(1);
    LINE *result = (LINE *) palloc(sizeof(LINE));

    // Validate that points are distinct
    if (point_eq_point(pt1, pt2))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("invalid line specification: must be two distinct points")));

    // Construct line from first point and slope between points
    line_construct(result, pt1, point_sl(pt1, pt2));

    PG_RETURN_LINE_P(result);
}
```
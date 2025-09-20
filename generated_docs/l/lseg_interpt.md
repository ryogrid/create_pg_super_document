# lseg_interpt

## Location
[src/backend/utils/adt/geo_ops.c:2361-2389](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L2361-L2389)

## Overview
PostgreSQL function that calculates the intersection point between two line segments and returns it as a Point.

## Definition

```c
Datum
lseg_interpt(PG_FUNCTION_ARGS)
```
## Detailed Description
This is a PostgreSQL function interface that wraps the internal lseg_interpt_lseg() function to provide intersection point calculation between two line segments. The function takes two line segments as arguments, allocates memory for a result point, and calls the internal intersection calculation function. If the segments intersect, it returns the intersection point; otherwise, it returns NULL. This function serves as the SQL-callable interface for line segment intersection point calculations.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_LSEG_P (retrieve line segment arguments)
  - [palloc](../p/palloc.md) (memory allocation)
  - [lseg_interpt_lseg](lseg_interpt_lseg.md) (internal intersection calculation)
  - PG_RETURN_NULL (return NULL value)
  - PG_RETURN_POINT_P (return point result)
- Called from:
  - [interpt_pp](../i/interpt_pp.md) (in test regression code)

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:2361-2389
- This is a PostgreSQL function that can be called from SQL queries
- Acts as a wrapper around the internal lseg_interpt_lseg() function
- Returns NULL when line segments don't intersect, following PostgreSQL's NULL handling conventions
- Memory allocation uses PostgreSQL's palloc system for proper memory management integration
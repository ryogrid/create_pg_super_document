# point_add

## Location
[src/backend/utils/adt/geo_ops.c:4119-4133](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4119-L4133)

## Overview
A PostgreSQL function that provides the SQL-callable interface for adding two geometric points together.

## Definition

```c
struct(result,
					float8_mi(pt1->x, pt2->x),
					float8_mi(pt1->y, pt2->y));
```
## Detailed Description
This function serves as the PostgreSQL SQL function interface for point addition operations. It extracts two Point arguments from the function call arguments using PostgreSQL's function calling convention, allocates memory for the result, delegates the actual computation to point_add_point, and returns the result in PostgreSQL's Datum format. This function enables the '+' operator for point data types in SQL queries.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  - Argument 0: First Point operand (accessed via )
  - Argument 1: Second Point operand (accessed via )

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P
  - [palloc](palloc.md)
  - [point_add_point](point_add_point.md)
  - PG_RETURN_POINT_P
  - [Point](../P/Point.md) (data type)
- Called from (representative examples):
  - SQL queries using point addition operator
  - PostgreSQL function call infrastructure

## Notes and Other Information
- This is a PostgreSQL internal function that implements the '+' operator for point data types
- Uses PostgreSQL's memory allocation (palloc) to ensure proper memory management within the database context
- Returns a Datum which is PostgreSQL's generic data type for SQL function returns
- The function signature follows PostgreSQL's V1 calling convention for internal functions
- Memory allocated with palloc will be automatically freed at the end of the current memory context
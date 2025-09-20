# pt_contained_poly

## Location
[src/backend/utils/adt/geo_ops.c:4017-4026](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4017-L4026)

## Overview
PostgreSQL function that determines whether a point is contained within a polygon, essentially the reverse argument order version of poly_contain_pt.

## Definition

```c
Datum
pt_contained_poly(PG_FUNCTION_ARGS)
```
## Detailed Description
This function tests whether a given point lies inside a polygon, using the same underlying algorithm as poly_contain_pt but with reversed argument order. It extracts a point and a polygon from the function arguments, then calls the point_inside function to perform the actual geometric computation. The function provides a different operator syntax for the same containment test - where pt_contained_poly tests "point @ polygon" while poly_contain_pt tests "polygon ~ point". Both functions utilize the same point_inside algorithm that works with the polygon's vertex array and vertex count.

## Parameters / Member Variables
- : PostgreSQL function call context containing two arguments
  - Argument 0: The point (Point) to test for containment
  - Argument 1: The polygon (POLYGON) to test containment within

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POINT_P (extract point argument)
  - PG_GETARG_POLYGON_P (extract polygon argument)
  - [point_inside](point_inside.md) (perform point-in-polygon test)
  - PG_RETURN_BOOL (return boolean result)
- Called from (representative examples):
  - No direct references found in the codebase (likely used via SQL operator @)

## Notes and Other Information
- The function is part of PostgreSQL's geometric data type operations in src/backend/utils/adt/geo_ops.c
- Provides the same functionality as poly_contain_pt but with arguments in reversed order
- The point_inside function handles the mathematical complexity of determining point containment within polygon boundaries
- Located at src/backend/utils/adt/geo_ops.c:4017-4026
- No explicit memory management is needed as the function doesn't create copies of the input data
- Supports different SQL operator syntaxes for point-in-polygon testing
# circle_overlap

## Location
[src/backend/utils/adt/geo_ops.c:4764-4776](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4764-L4776)

## Overview
Determines whether two circles overlap by comparing the distance between their centers against the sum of their radii.

## Definition
```c
Datum circle_overlap(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_overlap` function tests whether two CIRCLE objects overlap or intersect. The geometric principle used is straightforward: two circles overlap if and only if the distance between their centers is less than or equal to the sum of their radii.

The function implements this test by:
1. Calculating the distance between the two circle centers using `point_dt`
2. Computing the sum of the two radii using `float8_pl`
3. Comparing these values using `FPle` (floating-point less-than-or-equal)

This function serves as the implementation for PostgreSQL's overlap operator `&&` when applied to circle geometric types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `circle1`: First CIRCLE object (accessed via PG_GETARG_CIRCLE_P(0))
  - `circle2`: Second CIRCLE object (accessed via PG_GETARG_CIRCLE_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P: Extracts CIRCLE arguments from function call
  - [point_dt](../p/point_dt.md): Calculates distance between two points (circle centers)
  - [float8_pl](../f/float8_pl.md): Adds two floating-point values (radii sum)
  - [FPle](../F/FPle.md): Floating-point less-than-or-equal comparison
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using the `&&` operator for circles
  - Spatial indexing operations (GiST, SP-GiST)
  - Geometric intersection queries

## Notes and Other Information
- Part of PostgreSQL's geometric data type system for 2D circle operations
- Used as the underlying implementation for the overlap operator `&&` in SQL
- Critical for spatial indexing and query optimization in geometric databases
- The algorithm handles edge cases where circles exactly touch (distance equals sum of radii) as overlapping
- Follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Located alongside other circle relational operators in the geometric operations module
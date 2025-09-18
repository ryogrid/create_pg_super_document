# circle_right

## Location
[src/backend/utils/adt/geo_ops.c:4801-4813](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4801-L4813)

## Overview
Tests whether the first circle is positioned strictly to the right of the second circle with no overlap, ensuring complete horizontal separation.

## Definition
```c
Datum circle_right(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_right` function determines if circle1 is positioned entirely to the right of circle2 with no touching or overlap. This is a strict positional relationship test that requires complete horizontal separation between the two circles, mirroring the functionality of `circle_left` but in the opposite direction.

The comparison works by:
1. Calculating the leftmost point of circle1 as `center.x - radius`
2. Calculating the rightmost point of circle2 as `center.x + radius`
3. Testing if the leftmost point of circle1 is strictly greater than the rightmost point of circle2

This strict inequality ensures that there is actual space between the circles - they neither touch nor overlap. This operator is fundamental for:
- Spatial partitioning and range queries
- Non-overlapping spatial arrangements
- Geometric constraint checking
- Spatial indexing optimizations

The function serves as the implementation for PostgreSQL's `>>` operator for circle types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `circle1`: First CIRCLE object being tested for right positioning (accessed via PG_GETARG_CIRCLE_P(0))
  - `circle2`: Second CIRCLE object providing the reference position (accessed via PG_GETARG_CIRCLE_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P: Extracts CIRCLE arguments from function call
  - [float8_mi](../f/float8_mi.md): Subtracts floating-point values (center.x - radius for circle1)
  - [float8_pl](../f/float8_pl.md): Adds floating-point values (center.x + radius for circle2)
  - [FPgt](../F/FPgt.md): Floating-point strictly greater-than comparison
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using the `>>` operator for circles
  - Spatial partitioning algorithms
  - Non-overlapping constraint validation
  - Spatial indexing and query optimization

## Notes and Other Information
- Part of PostgreSQL's geometric data type system for 2D circle operations
- Used as the underlying implementation for the strictly-right operator `>>` in SQL
- Requires complete separation unlike potential overright operators which would allow touching
- Essential for spatial queries requiring non-overlapping geometric arrangements
- Follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Located alongside other circle positional operators in the geometric operations module
- Complementary to `circle_left` function, providing the opposite directional test
- Critical for applications requiring strict spatial separation constraints
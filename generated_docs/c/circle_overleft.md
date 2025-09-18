# circle_overleft

## Location
src/backend/utils/adt/geo_ops.c: 4777 - 4788

## Overview
Tests whether the right edge of the first circle is at or to the left of the right edge of the second circle, implementing a positional relationship for spatial indexing and ordering.

## Definition
```c
Datum circle_overleft(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_overleft` function determines if circle1 is positioned such that its rightmost point (right edge) does not extend beyond the rightmost point of circle2. This is a spatial relationship test commonly used in geometric indexing and spatial queries.

The comparison works by:
1. Calculating the right edge of circle1 as `center.x + radius`
2. Calculating the right edge of circle2 as `center.x + radius`
3. Testing if the right edge of circle1 is less than or equal to the right edge of circle2

This operator is particularly useful for:
- R-tree and other spatial index operations
- Range queries involving horizontal positioning
- Spatial ordering and sorting operations

The function serves as the implementation for PostgreSQL's `&<` operator for circle types.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - `circle1`: First CIRCLE object whose right edge is being tested (accessed via PG_GETARG_CIRCLE_P(0))
  - `circle2`: Second CIRCLE object providing the reference right edge (accessed via PG_GETARG_CIRCLE_P(1))

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P: Extracts CIRCLE arguments from function call
  - float8_pl: Adds floating-point values (center.x + radius for each circle)
  - FPle: Floating-point less-than-or-equal comparison
  - PG_RETURN_BOOL: Returns boolean result to PostgreSQL
- Called from (representative examples):
  - SQL queries using the `&<` operator for circles
  - Spatial indexing operations (GiST, SP-GiST)
  - Range and ordering queries in geometric applications

## Notes and Other Information
- Part of PostgreSQL's geometric data type system for 2D circle operations
- Used as the underlying implementation for the overlap-left operator `&<` in SQL
- Essential for spatial indexing strategies that rely on bounding rectangle comparisons
- The operator name 'overleft' reflects that it tests for 'overlap to the left' positioning
- Follows PostgreSQL's standard function interface using PG_FUNCTION_ARGS and PG_RETURN_BOOL macros
- Located alongside other circle positional operators in the geometric operations module
- Critical for efficient spatial query processing and index-based optimizations
# circle_overright

## Location
[src/backend/utils/adt/geo_ops.c:4814-4825](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4814-L4825)

## Overview
Tests whether the left edge of the first circle is at or to the right of the left edge of the second circle, implementing the PostgreSQL "&>" geometric operator for circles.

## Definition
Datum circle_overright(PG_FUNCTION_ARGS)

## Detailed Description
The circle_overright function implements the "overright" or "does not extend to left of" geometric operator for circle types in PostgreSQL. It compares the leftmost points of two circles by computing the difference between each circles center x-coordinate and its radius. The function returns true if the left edge of the first circle is positioned at or to the right of the left edge of the second circle.

The comparison uses floating-point arithmetic with appropriate precision handling through the FPge (floating-point greater-than-or-equal) function to ensure consistent results across different platforms.

## Parameters / Member Variables
- circle1 (PG_GETARG_CIRCLE_P(0)): Pointer to the first CIRCLE structure to compare
- circle2 (PG_GETARG_CIRCLE_P(1)): Pointer to the second CIRCLE structure to compare

## Dependencies
- Functions called/Symbols referenced:
  - CIRCLE (type definition)
  - PG_GETARG_CIRCLE_P (argument extraction macro)
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison)
  - [float8_mi](../f/float8_mi.md) (floating-point subtraction)
  - PG_RETURN_BOOL (return value macro)
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- Located in src/backend/utils/adt/geo_ops.c:4814-4825
- Part of PostgreSQLs geometric data type operations
- The function calculates the leftmost point of each circle as (center.x - radius)
- Uses PostgreSQLs standard function calling convention with PG_FUNCTION_ARGS
- Returns a boolean result indicating the spatial relationship between the circles
- This operator is typically used in geometric queries and spatial indexing operations
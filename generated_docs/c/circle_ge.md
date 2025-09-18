# circle_ge

## Location
[src/backend/utils/adt/geo_ops.c:4948-4964](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4948-L4964)

## Overview
Compares two circles and returns true if the first circle has a greater than or equal area compared to the second circle.

## Definition
```c
Datum circle_ge(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_ge` function implements the "greater than or equal" comparison operator for PostgreSQL's CIRCLE data type. It compares two circles based on their areas using floating-point comparison with epsilon tolerance. The function retrieves two CIRCLE arguments from the PostgreSQL function call interface, calculates their respective areas using the `circle_ar` helper function, and performs a floating-point "greater than or equal" comparison using `FPge` which accounts for floating-point precision issues by using an epsilon tolerance.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: First CIRCLE pointer (circle1)
  - Argument 1: Second CIRCLE pointer (circle2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P (retrieves CIRCLE arguments)
  - [circle_ar](circle_ar.md) (calculates circle area)
  - [FPge](../F/FPge.md) (floating-point greater-than-or-equal comparison with epsilon tolerance)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operators
- Uses area-based comparison rather than radius or diameter comparison
- Implements floating-point comparison with epsilon tolerance to handle precision issues
- Located in src/backend/utils/adt/geo_ops.c:4948-4964
- The comparison is based on the mathematical area formula: π × radius²
- Complements the circle_le function by providing the inverse comparison operation
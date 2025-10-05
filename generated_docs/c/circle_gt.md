# circle_gt

## Location
[src/backend/utils/adt/geo_ops.c:4930-4938](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4930-L4938)

## Overview
Compares two circles and returns true if the first circle has a greater area than the second circle.

## Definition

```c
Datum
circle_gt(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function implements the "greater than" comparison operator for PostgreSQL's CIRCLE data type. It compares two circles based on their areas using floating-point comparison with epsilon tolerance. The function retrieves two CIRCLE arguments from the PostgreSQL function call interface, calculates their respective areas using the  helper function, and performs a floating-point "greater than" comparison using  which accounts for floating-point precision issues by using an epsilon tolerance.

## Parameters / Member Variables
- : Standard PostgreSQL function argument macro that provides access to function arguments
  - Argument 0: First CIRCLE pointer (circle1)
  - Argument 1: Second CIRCLE pointer (circle2)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_CIRCLE_P (retrieves CIRCLE arguments)
  - [circle_ar](circle_ar.md) (calculates circle area)
  - [FPgt](../F/FPgt.md) (floating-point greater-than comparison with epsilon tolerance)
  - PG_RETURN_BOOL (returns boolean result)
- Called from (representative examples):
  - No direct references found in codebase

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type operators
- Uses area-based comparison rather than radius or diameter comparison
- Implements floating-point comparison with epsilon tolerance to handle precision issues
- Located in src/backend/utils/adt/geo_ops.c:4930-4938
- The comparison is based on the mathematical area formula: π × radius²

## Simplified Source

```c
Datum circle_gt(PG_FUNCTION_ARGS) {
    CIRCLE *circle1 = PG_GETARG_CIRCLE_P(0);
    CIRCLE *circle2 = PG_GETARG_CIRCLE_P(1);

    // Compare areas: circle1.area > circle2.area
    PG_RETURN_BOOL(FPgt(circle_ar(circle1), circle_ar(circle2)));
}
```
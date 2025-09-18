# circle_eq

## Location
src/backend/utils/adt/geo_ops.c: 4903 - 4911

## Overview
Tests whether two circles have equal areas within PostgreSQL's floating-point accuracy constraints.

## Definition
```c
Datum circle_eq(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_eq` function compares two circles for area equality. Rather than comparing the circles' geometric properties directly, it computes the area of each circle using `circle_ar` and performs a floating-point equality comparison using `FPeq`. This approach accounts for PostgreSQL's accuracy constraints when dealing with floating-point arithmetic in geometric calculations.

## Parameters / Member Variables
- `circle1`: First circle argument obtained via `PG_GETARG_CIRCLE_P(0)`
- `circle2`: Second circle argument obtained via `PG_GETARG_CIRCLE_P(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CIRCLE_P`: Macro to extract CIRCLE argument from function call
  - `circle_ar`: Function to calculate the area of a circle
  - `FPeq`: Floating-point equality comparison with accuracy tolerance
  - `PG_RETURN_BOOL`: Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function implements the \=\ operator for circle types in PostgreSQL
- Uses area-based comparison rather than direct geometric property comparison
- Employs floating-point accuracy constraints via `FPeq` for reliable equality testing
- Located in `src/backend/utils/adt/geo_ops.c:4903-4911`
- Part of PostgreSQL's geometric data type comparison operators
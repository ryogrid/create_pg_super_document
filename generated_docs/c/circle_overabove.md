# circle_overabove

## Location
[src/backend/utils/adt/geo_ops.c:4889-4902](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L4889-L4902)

## Overview
Tests whether the lower edge of one circle is at or above the lower edge of another circle.

## Definition
```c
Datum circle_overabove(PG_FUNCTION_ARGS)
```

## Detailed Description
The `circle_overabove` function performs a positional comparison between two circles to determine if the first circle's lower edge (center.y - radius) is at or above the second circle's lower edge. This is a geometric operator used in PostgreSQL's spatial data types for circle positioning queries. The function returns true if circle1's lowermost point has a y-coordinate greater than or equal to circle2's lowermost point.

## Parameters / Member Variables
- `circle1`: First circle argument obtained via `PG_GETARG_CIRCLE_P(0)`
- `circle2`: Second circle argument obtained via `PG_GETARG_CIRCLE_P(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_CIRCLE_P`: Macro to extract CIRCLE argument from function call
  - [FPge](../F/FPge.md): Floating-point greater-than-or-equal comparison
  - [float8_mi](../f/float8_mi.md): Floating-point subtraction function
  - `PG_RETURN_BOOL`: Macro to return boolean result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function implements the "|&>` operator for circle types in PostgreSQL
- Uses floating-point arithmetic with proper precision handling via `FPge` and `float8_mi`
- Located in `src/backend/utils/adt/geo_ops.c:4889-4902`
- Part of PostgreSQL's geometric data type operators for spatial queries
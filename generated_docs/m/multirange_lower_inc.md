# multirange_lower_inc

## Location
[src/backend/utils/adt/multirangetypes.c:1565-1583](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/multirangetypes.c#L1565-L1583)

## Overview
Tests whether the lower bound of a multirange is inclusive, returning true if the lower bound value is included in the range, false if it's exclusive, and false for empty multiranges.

## Definition
```c
Datum multirange_lower_inc(PG_FUNCTION_ARGS)
```

## Detailed Description
This function determines the inclusivity of the lower bound of a multirange. It first checks if the multirange is empty, returning false in that case since empty ranges have no meaningful bounds. For non-empty multiranges, it retrieves the bounds of the first range (index 0) within the multirange, which represents the overall lower bound since multiranges store ranges in sorted order. The function then returns the inclusivity flag of the lower bound, indicating whether the boundary value itself is part of the range.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention containing:
  - Arg 0: Input multirange (MultirangeType) to check lower bound inclusivity

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_MULTIRANGE_P
  - MultirangeIsEmpty
  - [multirange_get_typcache](multirange_get_typcache.md)
  - MultirangeTypeGetOid
  - [multirange_get_bounds](multirange_get_bounds.md)
  - PG_RETURN_BOOL
  - MultirangeType
  - RangeBound
- Called from (representative examples):
  - No direct references found (likely used via SQL function calls)

## Notes and Other Information
- Returns false for empty multiranges since they have no meaningful bounds
- Uses index 0 to get the bounds of the first range, which contains the overall lower bound of the multirange
- The inclusivity property is crucial for understanding whether boundary values are part of the range
- Range inclusivity affects operations like containment checks, overlaps, and intersections
- Complements other bound-checking functions like multirange_lower, multirange_upper, and multirange_upper_inc
- Essential for precise range arithmetic and boundary condition handling in PostgreSQL range operations
- The lower.inclusive field is a boolean that directly indicates the inclusivity status
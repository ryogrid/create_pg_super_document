# elem_contained_by_range

## Location
[src/backend/utils/adt/rangetypes.c:557-572](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L557-L572)

## Overview
This PostgreSQL function determines whether an element is contained within a range type, implementing the "contained by" operator (<@) for element-to-range comparisons.

## Definition

```c
Datum
elem_contained_by_range(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that checks if a given element value is contained within the boundaries of a range type. It serves as the implementation for the "contained by" operator (<@) when the left operand is an element and the right operand is a range. The function extracts the element value and range from the function arguments, obtains the appropriate type cache information for the range's element type, and delegates the actual containment check to the internal  function.

## Parameters / Member Variables
- : Standard PostgreSQL function argument structure containing:
  - Argument 0: The element value (Datum) to check for containment
  - Argument 1: The range type (RangeType *) to check against

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_DATUM
  - PG_GETARG_RANGE_P
  - [range_get_typcache](../r/range_get_typcache.md)
  - RangeTypeGetOid
  - [range_contains_elem_internal](../r/range_contains_elem_internal.md)
  - PG_RETURN_BOOL
- Called from (representative examples):
  - No direct callers found (likely called through PostgreSQL's function dispatch system)

## Notes and Other Information
- This function is typically invoked through SQL expressions using the <@ operator (e.g., )
- The actual containment logic is implemented in , making this function primarily a PostgreSQL function interface wrapper
- The function uses PostgreSQL's type cache system to handle different range element types efficiently
- Located in src/backend/utils/adt/rangetypes.c:557-572
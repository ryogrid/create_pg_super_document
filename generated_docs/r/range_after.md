# range_after

## Location
[src/backend/utils/adt/rangetypes.c:727-756](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L727-L756)

## Overview
Determines whether the first range is strictly positioned after (to the right of) the second range.

## Definition

```c
Datum
range_after(PG_FUNCTION_ARGS)
```
## Detailed Description
This function implements the PostgreSQL range ">>" (strictly after) operator. It checks if the first range is completely positioned after the second range with no overlap or adjacency. The function is a PostgreSQL SQL function wrapper that extracts range arguments and delegates the actual comparison logic to .

The function validates that both ranges are of the same type and returns false if either range is empty, as empty ranges have no positional relationship with other ranges.

## Parameters / Member Variables
- Uses  macro to access function arguments:
  -  (argument 0): First RangeType to compare
  -  (argument 1): Second RangeType to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - [range_get_typcache](range_get_typcache.md)
  - RangeTypeGetOid
  - [range_after_internal](range_after_internal.md)
- Called from (representative examples):
  - No direct callers found (SQL operator function)

## Notes and Other Information
- This function serves as the SQL-callable wrapper for the ">>" range operator
- Empty ranges are never considered to be after any other range
- Range types must match or an error is raised
- The actual comparison logic compares the lower bound of the first range with the upper bound of the second range
- Located in src/backend/utils/adt/rangetypes.c:727-756
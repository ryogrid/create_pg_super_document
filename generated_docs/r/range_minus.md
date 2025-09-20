# range_minus

## Location
[src/backend/utils/adt/rangetypes.c:972-992](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/rangetypes.c#L972-L992)

## Overview
The range_minus function computes the set difference between two range values, returning the portion of the first range that does not overlap with the second range.

## Definition

```c
Datum
range_minus(PG_FUNCTION_ARGS)
```
## Detailed Description
The range_minus function is a PostgreSQL built-in function that implements the range difference operation (A - B). It takes two range arguments of the same type and returns a new range representing the elements that are in the first range but not in the second range. The function performs type validation to ensure both input ranges are of the same type, then delegates the actual computation to range_minus_internal. If the result would be empty, the function returns NULL.

## Parameters / Member Variables
- Function uses PG_FUNCTION_ARGS macro to access arguments:
  - : First range argument (minuend) obtained via PG_GETARG_RANGE_P(0)
  - : Second range argument (subtrahend) obtained via PG_GETARG_RANGE_P(1)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_RANGE_P
  - RangeTypeGetOid
  - [range_get_typcache](range_get_typcache.md)
  - [range_minus_internal](range_minus_internal.md)
  - PG_RETURN_RANGE_P
- Called from (representative examples):
  - No direct references found (likely called via SQL function dispatch)

## Notes and Other Information
- The function validates that both input ranges have the same type using RangeTypeGetOid comparison
- Returns NULL if the result would be empty
- Throws an error if range types don't match
- The actual computation is delegated to range_minus_internal for modularity
- Part of PostgreSQL's range type system introduced in version 9.2
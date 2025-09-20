# poly_above

## Location
[src/backend/utils/adt/geo_ops.c:3671-3693](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L3671-L3693)

## Overview
Tests whether polygon A is strictly above polygon B by ensuring there is no vertical overlap between them.

## Definition

```c
Datum
poly_above(PG_FUNCTION_ARGS)
```
## Detailed Description
The `poly_above` function determines if polygon A is positioned strictly above polygon B with no vertical overlap. This is accomplished by comparing the lowest y-coordinate of polygon A with the highest y-coordinate of polygon B. The function returns true only if the bottom edge of polygon A is above the top edge of polygon B.

This function is part of PostgreSQL's geometric operators and is used in spatial queries to test strict vertical separation between polygons. Unlike `poly_overabove`, this function requires complete separation rather than allowing touching or overlapping boundaries.

The implementation uses bounding box comparisons for efficient computation, making it suitable for spatial indexing operations.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument structure containing:
  - First argument: POLYGON pointer (polygon A)
  - Second argument: POLYGON pointer (polygon B)

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P: Extracts polygon arguments from function call
  - PG_FREE_IF_COPY: Frees memory for toasted inputs
  - PG_RETURN_BOOL: Returns boolean result
- Called from (representative examples):
  - No direct references found in current codebase

## Notes and Other Information
- The function performs memory management by freeing toasted inputs to prevent memory leaks
- Uses strict inequality (>) to ensure complete vertical separation
- Compares boundbox.low.y of polygon A with boundbox.high.y of polygon B
- Part of PostgreSQL's geometric data type operator family for spatial relationships
- Essential for R-tree index operations requiring strict spatial ordering
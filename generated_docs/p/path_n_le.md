# path_n_le

## Location
[src/backend/utils/adt/geo_ops.c:1580-1588](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1580-L1588)

## Overview
A PostgreSQL function that compares two PATH objects to determine if the first path has fewer or equal number of points than the second path.

## Definition
```c
Datum path_n_le(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_n_le` function implements the "less than or equal" comparison operator for PATH objects based on their number of points. It extracts two PATH arguments from the function call, compares their `npts` fields, and returns a boolean result indicating whether the first path has fewer or equal points than the second path. This function is part of PostgreSQL's geometric data type operations and is typically used in SQL queries with the `<=` operator when comparing paths by their point count.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention that provides access to function arguments
- First argument (index 0): Pointer to the first PATH object to compare
- Second argument (index 1): Pointer to the second PATH object to compare

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P: Macro to extract PATH argument from function call
  - PG_RETURN_BOOL: Macro to return boolean result
  - [PATH](../P/PATH.md): Geometric path data type structure

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL operator framework)

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type support
- The comparison is based solely on the number of points (`npts` field) in each path
- Returns true if the first path has fewer or equal points than the second path
- Used internally by PostgreSQL's SQL operator system when the `<=` operator is applied to path objects
- Location: src/backend/utils/adt/geo_ops.c:1580-1588
# path_n_ge

## Location
src/backend/utils/adt/geo_ops.c: 1589 - 1601

## Overview
A PostgreSQL function that compares two PATH objects to determine if the first path has greater or equal number of points than the second path.

## Definition
```c
Datum path_n_ge(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_n_ge` function implements the "greater than or equal" comparison operator for PATH objects based on their number of points. It extracts two PATH arguments from the function call, compares their `npts` fields, and returns a boolean result indicating whether the first path has greater or equal points than the second path. This function is part of PostgreSQL's geometric data type operations and is typically used in SQL queries with the `>=` operator when comparing paths by their point count.

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
- Returns true if the first path has greater or equal points than the second path
- Used internally by PostgreSQL's SQL operator system when the `>=` operator is applied to path objects
- Location: src/backend/utils/adt/geo_ops.c:1589-1601
# path_close

## Location
[src/backend/utils/adt/geo_ops.c:1627-1636](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1627-L1636)

## Overview
The  function converts an open path to a closed path by setting the closed flag to true.

## Definition

```c
Datum
path_close(PG_FUNCTION_ARGS)
```
## Detailed Description
The  function is a PostgreSQL built-in function that takes a PATH geometric type as input and returns a copy of that path with its closed flag set to true. This effectively converts an open path (polyline) into a closed path (polygon). The function creates a copy of the input path to avoid modifying the original data structure, then sets the  field to true before returning the modified path.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that contains the input path parameter
## Dependencies
- Functions called/Symbols referenced:
  - : Geometric path data type structure
  - : Macro to get a copy of the path argument from function parameters
  - : Macro to return a path result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically used in SQL queries as  to convert open paths to closed paths
- The function creates a copy of the input path, so the original path remains unchanged
- Setting the closed flag affects how the path is interpreted geometrically (as a polygon vs polyline)
- Part of PostgreSQL's geometric data type operations suite
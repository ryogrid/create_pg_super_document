# path_open

## Location
[src/backend/utils/adt/geo_ops.c:1637-1652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1637-L1652)

## Overview
The `path_open` function converts a closed path to an open path by setting the closed flag to false.

## Definition
```c
Datum path_open(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_open` function is a PostgreSQL built-in function that takes a PATH geometric type as input and returns a copy of that path with its closed flag set to false. This effectively converts a closed path (polygon) into an open path (polyline). The function creates a copy of the input path to avoid modifying the original data structure, then sets the `closed` field to false before returning the modified path.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function argument macro that contains the input path parameter

## Dependencies
- Functions called/Symbols referenced:
  - [PATH](../P/PATH.md): Geometric path data type structure
  - `PG_GETARG_PATH_P_COPY`: Macro to get a copy of the path argument from function parameters
  - `PG_RETURN_PATH_P`: Macro to return a path result
- Called from (representative examples):
  - No direct references found in the codebase

## Notes and Other Information
- This function is typically used in SQL queries as `path_open(path)` to convert closed paths to open paths
- The function creates a copy of the input path, so the original path remains unchanged
- Setting the closed flag affects how the path is interpreted geometrically (as a polyline vs polygon)
- Part of PostgreSQL's geometric data type operations suite
- Complementary function to `path_close`

## Simplified Source

```c
Datum path_open(PG_FUNCTION_ARGS) {
    // Get a copy of the PATH object from function argument
    PATH *path = PG_GETARG_PATH_P_COPY(0);

    // Set the closed flag to false (convert closed path to open)
    path->closed = false;

    // Return the modified path
    PG_RETURN_PATH_P(path);
}
```
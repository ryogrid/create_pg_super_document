# path_isopen

## Location
[src/backend/utils/adt/geo_ops.c:1610-1617](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1610-L1617)

## Overview
A PostgreSQL function that determines whether a given PATH object represents an open path (as opposed to a closed path).

## Definition
```c
Datum path_isopen(PG_FUNCTION_ARGS)
```

## Detailed Description
The `path_isopen` function is a conversion operator that checks if a PATH object is open by testing the negation of the `closed` field. An open path is one where the first and last points are not connected, representing line segments or curves with distinct start and end points. This function provides the logical opposite of `path_isclosed` and can be used in SQL queries to filter or categorize paths based on their geometric properties. It is part of PostgreSQL's geometric data type support under the conversion operators category.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL function calling convention that provides access to function arguments
- First argument (index 0): Pointer to the PATH object to check

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_PATH_P: Macro to extract PATH argument from function call
  - PG_RETURN_BOOL: Macro to return boolean result
  - [PATH](../P/PATH.md): Geometric path data type structure

- Called from (representative examples):
  - No direct references found in the codebase (likely called through SQL function framework)

## Notes and Other Information
- This function is the logical complement of `path_isclosed`
- Returns the negation of the boolean value stored in the `closed` field of the PATH structure
- An open path typically represents line segments, curves, or polygonal chains without forming a complete loop
- A closed path represents polygonal shapes where the boundary forms a complete loop
- Used in SQL queries to test path properties, e.g., `WHERE path_isopen(mypath)`
- Provides convenient access to test for open paths without requiring negation in SQL queries
- Location: src/backend/utils/adt/geo_ops.c:1610-1617

## Simplified Source

```c
Datum path_isopen(PG_FUNCTION_ARGS) {
    // Extract PATH object from function argument
    PATH *path = PG_GETARG_PATH_P(0);

    // Return negation of closed flag (open = !closed)
    PG_RETURN_BOOL(!path->closed);
}
```
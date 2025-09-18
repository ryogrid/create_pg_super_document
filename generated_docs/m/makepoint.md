# makepoint

## Location
src/tutorial/funcs.c: 47 - 63

## Overview
A PostgreSQL C function that creates a new Point by combining the x-coordinate from one Point and the y-coordinate from another Point, demonstrating composite type manipulation in PostgreSQL.

## Definition
```c
Datum makepoint(PG_FUNCTION_ARGS)
```

## Detailed Description
The `makepoint` function is a PostgreSQL C function that takes two Point arguments and creates a new Point by extracting the x-coordinate from the first Point and the y-coordinate from the second Point. This function demonstrates how to work with PostgreSQL's composite data types (specifically the built-in Point type) and shows proper memory allocation for returning composite values. The function uses `palloc` for memory allocation, which is PostgreSQL's memory management system that ensures proper cleanup.

## Parameters / Member Variables
- `PG_FUNCTION_ARGS`: Standard PostgreSQL macro that provides access to function arguments and context information
  - First argument: A Point structure accessed via `PG_GETARG_POINT_P(0)`
  - Second argument: A Point structure accessed via `PG_GETARG_POINT_P(1)`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POINT_P`: Macro to extract a Point argument (used twice for both parameters)
  - [Point](../P/Point.md): PostgreSQL's built-in geometric Point data type
  - [palloc](../p/palloc.md): PostgreSQL's memory allocation function
  - `PG_RETURN_POINT_P`: Macro to return a Point value
  - `PG_FUNCTION_INFO_V1`: Macro for function metadata (referenced at line 61)
- Called from (representative examples):
  - [add_one_float8](../a/add_one_float8.md): Referenced from the add_one_float8 function context

## Notes and Other Information
- Located in `src/tutorial/funcs.c:47-63`
- This is a tutorial example function demonstrating composite type handling
- Uses PostgreSQL's memory management with `palloc` for proper memory allocation
- Demonstrates coordinate manipulation with PostgreSQL's Point geometry type
- The function creates a hybrid point by mixing coordinates from two input points
- Follows PostgreSQL's version 1 calling convention
- Shows proper handling of pass-by-reference composite types
# point_out

## Location
src/backend/utils/adt/geo_ops.c: 1842 - 1852

## Overview
Converts PostgreSQL's internal Point data structure into its string representation for output.

## Definition
```c
Datum point_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `point_out` function is responsible for converting PostgreSQL's internal Point data type back into a human-readable string format. This is the counterpart to `point_in` and handles the output conversion for display, logging, or client communication. The function uses the `path_encode` utility function with `PATH_NONE` parameter to format a single point, producing output in the standard "(x,y)" format.

## Parameters / Member Variables
- `pt`: Input Point structure containing x and y coordinates to be converted to string

## Dependencies
- Functions called/Symbols referenced:
  - `Point` - PostgreSQL's 2D point data structure
  - `PG_GETARG_POINT_P` - Macro for extracting Point argument from PostgreSQL function call
  - `PATH_NONE` - Constant indicating single point encoding (not a path)
  - `path_encode` - Utility function for encoding geometric objects to string format
  - `PG_RETURN_CSTRING` - Macro for returning C string from PostgreSQL functions
- Called from (representative examples):
  - No direct references found (likely called through PostgreSQL's type system)

## Notes and Other Information
- This function is part of PostgreSQL's geometric data type system
- Output function for the Point data type, typically registered in the PostgreSQL type system catalog
- Reuses the path encoding infrastructure to format single points consistently
- The resulting string format matches what `point_in` expects as input, ensuring round-trip compatibility
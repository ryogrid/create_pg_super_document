# line_in

## Location
src/backend/utils/adt/geo_ops.c: 979 - 1022

## Overview
Parses a string representation of a line and converts it to the internal LINE data structure. This function serves as the input function for the PostgreSQL line data type.

## Definition
```c
Datum line_in(PG_FUNCTION_ARGS)
```

## Detailed Description
The `line_in` function is the input converter for PostgreSQL's line geometric data type. It accepts string representations in two formats:
1. Standard line equation format: `{A,B,C}` representing the line equation Ax + By + C = 0
2. Two-point format: `[(x1,y1),(x2,y2)]` or `((x1,y1),(x2,y2))` representing a line through two distinct points

The function performs validation to ensure mathematical validity:
- For equation format: A and B coefficients cannot both be zero (would not represent a valid line)
- For point format: The two points must be distinct (identical points cannot define a line)

When given two points, the function internally converts them to the standard Ax + By + C = 0 form using the `line_construct` helper function with the calculated slope.

## Parameters / Member Variables
- `str`: Input C-string containing the line specification
- `escontext`: Error context for soft error handling
- Returns: `Datum` containing pointer to allocated LINE structure

## Dependencies
- Functions called/Symbols referenced:
  - `line_decode`: Parses equation format `{A,B,C}`
  - `path_decode`: Parses point format `[(x1,y1),(x2,y2)]`
  - `point_eq_point`: Checks if two points are identical
  - `line_construct`: Constructs line from point and slope
  - `lseg_sl`: Calculates slope of line segment
  - `FPzero`: Tests if floating-point value is zero
  - `ereturn`: Returns error in soft error context
- Called from (representative examples):
  - PostgreSQL type input system (no direct function references found)

## Notes and Other Information
- Part of PostgreSQL's geometric data type system in `src/backend/utils/adt/geo_ops.c`
- Supports both explicit equation format and implicit two-point format
- Uses soft error handling through `escontext` for graceful error reporting
- Memory allocation for LINE structure uses `palloc()`
- Line numbers: 979-1022 in geo_ops.c
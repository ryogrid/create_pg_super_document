# line_out

## Location
[src/backend/utils/adt/geo_ops.c:1023-1037](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/adt/geo_ops.c#L1023-L1037)

## Overview
Converts a LINE data structure to its string representation for output. This function serves as the output function for the PostgreSQL line data type.

## Definition
```c
Datum line_out(PG_FUNCTION_ARGS)
```

## Detailed Description
The `line_out` function is the output converter for PostgreSQL's line geometric data type. It takes an internal LINE structure containing the coefficients A, B, and C of the line equation Ax + By + C = 0 and formats them into a standardized string representation. The output format is `{A,B,C}` where each coefficient is formatted as a floating-point number using PostgreSQL's standard float8 output formatting.

The function uses `float8out_internal` to ensure consistent formatting of floating-point values according to PostgreSQL's conventions for numeric precision and representation.

## Parameters / Member Variables
- `line`: Input LINE structure pointer containing A, B, C coefficients
- Returns: `Datum` containing C-string in format `{A,B,C}`

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_LINE_P`: Extracts LINE pointer from function arguments
  - [float8out_internal](../f/float8out_internal.md): Converts float8 values to string representation
  - [psprintf](../p/psprintf.md): PostgreSQL's sprintf equivalent for formatted string creation
  - `PG_RETURN_CSTRING`: Returns C-string as Datum
- Called from (representative examples):
  - PostgreSQL type output system (no direct function references found)

## Notes and Other Information
- Part of PostgreSQL's geometric data type system in `src/backend/utils/adt/geo_ops.c`
- Uses delimiters LDELIM_L (`{`), DELIM (`,`), and RDELIM_L (`}`) for consistent formatting
- Output format matches the input format accepted by `line_in`
- Memory management handled by PostgreSQL's memory context system
- Line numbers: 1023-1037 in geo_ops.c
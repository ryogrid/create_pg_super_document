# poly_out

## Location
src/backend/utils/adt/geo_ops.c: 3459 - 3474

## Overview
The `poly_out` function converts PostgreSQLs internal POLYGON representation into a human-readable string format for external display and output.

## Definition
```c
Datum poly_out(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a PostgreSQL output function that handles the conversion from internal POLYGON data structure to external string representation. It produces output in the standard format "((f8,f8),...,(f8,f8))" where each (f8,f8) pair represents a points x and y coordinates as floating-point numbers.

The function is straightforward and delegates the actual formatting work to the `path_encode` function, specifying that the polygon should be treated as a closed path. This design promotes code reuse since polygons and closed paths share the same string representation format.

## Parameters / Member Variables
- Standard PostgreSQL function arguments accessed via:
  - `PG_GETARG_POLYGON_P(0)`: The input POLYGON structure to be converted to string format

## Dependencies
- Functions called/Symbols referenced:
  - `PG_GETARG_POLYGON_P`: Macro to extract POLYGON argument from function call
  - [path_encode](path_encode.md): Core function that handles the coordinate formatting
  - `PATH_CLOSED`: Constant indicating closed path formatting
  - `PG_RETURN_CSTRING`: Returns the formatted string result
- Called from (representative examples):
  - This is a PostgreSQL type output function, typically called by the SQL engine for display, COPY operations, and type conversions

## Notes and Other Information
- This is a PostgreSQL type output function registered in the system catalogs
- Very simple implementation that leverages existing path formatting infrastructure
- Produces standardized polygon string format compatible with `poly_in`
- The output format uses closed path notation with double parentheses
- Part of the geometric data types subsystem in PostgreSQL
- Ensures round-trip compatibility: poly_out(poly_in(string)) should preserve the polygon data
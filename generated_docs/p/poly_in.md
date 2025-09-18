# poly_in

## Location
src/backend/utils/adt/geo_ops.c: 3415 - 3458

## Overview
The `poly_in` function parses a string representation of a polygon and converts it into PostgreSQLs internal POLYGON data structure.

## Definition
```c
Datum poly_in(PG_FUNCTION_ARGS)
```

## Detailed Description
This is a PostgreSQL input function that handles the conversion from external string representation to internal POLYGON format. It supports multiple input formats including the standard "((x0,y0),...,(xn,yn))" format, a simplified "x0,y0,...,xn,yn" format, and the older style "(x1,...,xn,y1,...yn)" format.

The function performs several validation steps including checking for valid point count, preventing integer overflow when calculating memory requirements, and validating the parsed coordinates. After successful parsing, it calculates the polygons bounding box using `make_bound_box()` to optimize future spatial operations.

The function uses PostgreSQLs error context system to provide detailed error messages when parsing fails, making it user-friendly for debugging malformed input.

## Parameters / Member Variables
- Standard PostgreSQL function arguments accessed via:
  - `PG_GETARG_CSTRING(0)`: The input string containing the polygon representation
  - `fcinfo->context`: Error context for soft error handling

## Dependencies
- Functions called/Symbols referenced:
  - `[pair_count](pair_count.md)`: Counts coordinate pairs in the input string
  - `ereturn`: Error return macro for soft error handling
  - `[palloc0](palloc0.md)`: PostgreSQL memory allocation with zero initialization
  - `SET_VARSIZE`: Sets the variable-length structure size
  - `path_decode`: Core parsing function that extracts coordinates
  - `[make_bound_box](../m/make_bound_box.md)`: Calculates the polygons bounding box
  - `PG_RETURN_POLYGON_P`: Returns the polygon result
- Called from (representative examples):
  - This is a PostgreSQL type input function, typically called by the SQL parser and type conversion system

## Notes and Other Information
- This is a PostgreSQL type input function registered in the system catalogs
- Includes comprehensive integer overflow protection for large polygons
- Supports multiple polygon representation formats for compatibility
- Uses soft error handling (ereturn) to provide detailed error context
- The bounding box is automatically calculated for optimization purposes
- Memory is allocated using PostgreSQLs memory management system
- Part of the geometric data types subsystem in PostgreSQL
# poly_circle

## Location
src/backend/utils/adt/geo_ops.c: 5307 - 5336

## Overview
PostgreSQL SQL-callable function that converts a polygon to its approximate equivalent circle representation.

## Definition


## Detailed Description
The `poly_circle` function serves as the public PostgreSQL interface for converting polygon geometric objects to circle representations. It acts as a wrapper around the internal `poly_to_circle` function, handling the PostgreSQL function call protocol including parameter extraction, memory allocation, and return value handling.

The function extracts the input polygon from the PostgreSQL function arguments, allocates memory for the result circle, delegates the actual conversion logic to the `poly_to_circle` helper function, and returns the computed circle using PostgreSQL's return macros.

## Parameters / Member Variables
- Function arguments are accessed via PostgreSQL's `PG_FUNCTION_ARGS` macro:
  - Argument 0: POLYGON* - The input polygon to be converted to a circle

## Dependencies
- Functions called/Symbols referenced:
  - PG_GETARG_POLYGON_P (extract polygon argument from PostgreSQL function call)
  - palloc (PostgreSQL memory allocation)
  - poly_to_circle (internal function that performs the actual conversion)
  - PG_RETURN_CIRCLE_P (return circle result to PostgreSQL)
- Data types referenced:
  - POLYGON, CIRCLE (geometric data structures)

## Notes and Other Information
- This function follows the standard PostgreSQL function calling convention using the `PG_FUNCTION_ARGS` interface
- Memory for the result circle is allocated using PostgreSQL's memory context system via `palloc`
- The function is designed to be called from SQL as a geometric conversion operator
- The actual conversion algorithm is implemented in the separate `poly_to_circle` static function
- No input validation is performed at this level; validation and error handling are delegated to the underlying conversion function
- The function can be used in SQL queries for geometric computations involving polygon-to-circle conversions
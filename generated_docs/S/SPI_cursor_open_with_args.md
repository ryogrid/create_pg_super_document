# SPI_cursor_open_with_args

## Location
src/backend/executor/spi.c: 1472 - 1524

## Overview
SPI_cursor_open_with_args parses, plans, and opens a SQL query as a portal (cursor) in a single operation, providing a convenient interface for executing parameterized queries without pre-preparation.

## Definition
```c
Portal SPI_cursor_open_with_args(const char *name, const char *src, int nargs, Oid *argtypes, Datum *Values, const char *Nulls, bool read_only, int cursorOptions)
```

## Detailed Description
This function combines query parsing, planning, and cursor creation into a single operation, making it convenient for applications that don't need to reuse prepared plans. It performs the complete workflow of query processing:

1. **Argument Validation**: Validates that required parameters are provided and consistent
2. **SPI Context Management**: Begins an SPI call context and ensures proper cleanup
3. **Temporary Plan Creation**: Creates a transient _SPI_plan structure with the specified parameters
4. **Parameter Conversion**: Converts Datum/Nulls arrays to internal ParamListInfo format
5. **Query Preparation**: Parses and plans the SQL query string
6. **Portal Creation**: Opens the prepared plan as a cursor using the internal cursor opening function
7. **Cleanup**: Properly ends the SPI call context

This function is particularly useful for one-time query execution where plan reuse is not required, providing a simpler interface than the separate prepare/execute approach.

## Parameters / Member Variables
- `name`: Name to assign to the portal/cursor. Can be NULL for an unnamed portal.
- `src`: SQL query string to be parsed and executed. Must not be NULL.
- `nargs`: Number of parameters in the query. Must be >= 0.
- `argtypes`: Array of parameter type OIDs. Required if nargs > 0.
- `Values`: Array of parameter values as Datum. Required if nargs > 0.
- `Nulls`: String indicating null parameters ('n' for null, ' ' for non-null). Can be NULL if no parameters are null.
- `read_only`: Boolean flag indicating whether the cursor should be read-only.
- `cursorOptions`: Integer bitmask of cursor options (e.g., CURSOR_OPT_SCROLL).

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call (initialize SPI context)
  - [_SPI_convert_params](_SPI_convert_params.md) (convert parameters to internal format)
  - [_SPI_prepare_plan](_SPI_prepare_plan.md) (parse and plan the query)
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md) (create the actual portal)
  - _SPI_end_call (cleanup SPI context)
  - elog (error logging)
- Called from (representative examples):
  - Currently appears to be primarily used through the SPI interface header

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call
  - [_SPI_convert_params](_SPI_convert_params.md)
  - [_SPI_prepare_plan](_SPI_prepare_plan.md)
  - [SPI_cursor_open_internal](SPI_cursor_open_internal.md)
  - _SPI_end_call
- Called from (representative examples):
  - Available through SPI interface for direct use

## Notes and Other Information
- The function performs comprehensive argument validation, throwing errors for invalid combinations.
- Creates a temporary plan structure that doesn't need to be explicitly freed by the caller.
- Automatically handles SPI context management, ensuring proper cleanup even in error conditions.
- The cursorOptions parameter allows fine-grained control over cursor behavior (scrollable, holdable, etc.).
- More convenient than SPI_prepare followed by SPI_cursor_open when plan reuse is not needed.
- The function is fully self-contained, handling all aspects of query processing from parsing to cursor creation.
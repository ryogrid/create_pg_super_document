# _SPI_error_callback

## Location
src/backend/executor/spi.c: 2961 - 3006

## Overview
 is an error callback function that adds contextual information when a query invoked via SPI fails, providing better error reporting with query details.

## Definition


## Detailed Description
This function serves as an error callback handler within the SPI framework. When a query execution fails, this callback is invoked to enhance the error message with contextual information about the failing query. It handles syntax errors specially by converting them to internal syntax errors with position information, while other errors receive descriptive context based on the query type.

The function uses the SPICallbackArg structure to access the query string and parsing mode, then applies appropriate error context formatting based on whether it's a PL/pgSQL expression, assignment, or regular SQL statement.

## Parameters / Member Variables
- : Void pointer to SPICallbackArg structure containing query information and parsing mode

## Dependencies
- Functions called/Symbols referenced:
  - geterrposition: Gets current error position information
  - errposition: Sets error position to zero (clears external position)
  - internalerrposition: Sets internal error position for syntax errors
  - internalerrquery: Sets the query text for internal syntax errors
  - errcontext: Adds contextual information to error messages
- Called from (representative examples):
  - SPI_cursor_open_internal: Cursor opening operations
  - SPI_plan_get_cached_plan: Plan retrieval operations
  - _SPI_prepare_plan: Plan preparation operations
  - _SPI_prepare_oneshot_plan: One-shot plan preparation
  - _SPI_execute_plan: Plan execution operations

## Notes and Other Information
- Returns early if query string is NULL to handle uninitialized callback arguments
- Distinguishes between syntax errors (with position) and runtime errors
- Provides different context messages for PL/pgSQL expressions vs assignments vs regular SQL
- Uses RAW_PARSE_PLPGSQL_* constants to determine appropriate error context formatting
- Essential for debugging SPI-executed queries by providing meaningful error context
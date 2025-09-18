# SPI_prepare_cursor

## Location
[src/backend/executor/spi.c:866-901](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/executor/spi.c#L866-L901)

## Overview
SPI_prepare_cursor parses and plans a SQL query with parameter placeholders and cursor options, returning a reusable plan that can be executed as either a regular query or a cursor.

## Definition


## Detailed Description
SPI_prepare_cursor is the core preparation function that creates a prepared statement plan from a SQL query string with support for cursor-specific options. It validates parameters, initializes a plan structure with specified cursor options, calls the internal preparation routine, and copies the plan to the procedure context to make it persistent across SPI calls.

This function is the foundation for SPI_prepare (which calls it with cursorOptions=0) and provides the flexibility to specify cursor behavior such as parallel execution options. The resulting plan can be executed multiple times and supports both regular execution and cursor-based execution.

## Parameters / Member Variables
- `src`: const char * - The SQL query string to prepare (may contain parameter placeholders like , , etc.)
- `nargs`: int - Number of parameters expected by the query
- `argtypes`: Oid * - Array of PostgreSQL type OIDs for the parameters
- `cursorOptions`: int - Cursor behavior flags (e.g., CURSOR_OPT_PARALLEL_OK, CURSOR_OPT_HOLD, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - _SPI_begin_call
  - [_SPI_prepare_plan](_SPI_prepare_plan.md)
  - _SPI_make_plan_non_temp
  - _SPI_end_call
  - _SPI_plan
  - [SPIPlanPtr](SPIPlanPtr.md)
  - _SPI_PLAN_MAGIC
  - RAW_PARSE_DEFAULT
  - SPI_ERROR_ARGUMENT
- Called from (representative examples):
  - [SPI_prepare](SPI_prepare.md)

## Notes and Other Information
- Returns SPIPlanPtr on success, NULL on failure (check SPI_result for error details)
- The returned plan must be freed with SPI_freeplan when no longer needed
- Sets SPI_result to indicate success or specific error conditions
- Uses RAW_PARSE_DEFAULT for parsing mode, which handles standard SQL syntax
- The plan is copied to procedure context via _SPI_make_plan_non_temp to persist beyond the current SPI call
- Parameter placeholders in the SQL should be written as , , etc.
- cursorOptions can control parallel execution, holdability, and other cursor behaviors
- Validates that argtypes is provided when nargs > 0
- The magic number _SPI_PLAN_MAGIC is used for plan validation in subsequent operations
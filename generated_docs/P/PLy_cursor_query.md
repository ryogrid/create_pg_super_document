# PLy_cursor_query

## Location
src/pl/plpython/plpy_cursorobject.c: 78 - 140

## Overview
Creates a PL/Python cursor object from a SQL query string by preparing the query and opening a PostgreSQL portal.

## Definition


## Detailed Description
PLy_cursor_query creates a cursor object for executing and iterating through the results of a SQL query string. The function performs several key operations: allocates and initializes a PLyCursorObject, creates a dedicated memory context for the cursor, prepares the SQL query using SPI_prepare(), opens a portal using SPI_cursor_open(), and sets up the necessary infrastructure for converting PostgreSQL tuples to Python objects.

The function operates within a subtransaction to ensure proper error handling and resource cleanup. It validates the query string encoding, prepares the query plan, and creates a named portal that can be used for incremental result fetching. The portal is pinned to prevent premature cleanup and the portal name is stored in the cursor object for later access.

## Parameters / Member Variables
- : SQL query string to be executed through the cursor

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_New (Python C API)
  - PLy_current_execution_context
  - AllocSetContextCreate
  - PLy_input_setup_func
  - PLy_spi_subtransaction_begin
  - pg_verifymbstr
  - SPI_prepare
  - SPI_cursor_open
  - SPI_freeplan
  - MemoryContextStrdup
  - PinPortal
  - PLy_spi_subtransaction_commit
  - PLy_spi_subtransaction_abort
- Called from (representative examples):
  - PLy_cursor

## Notes and Other Information
- Creates a dedicated memory context "PL/Python cursor context" for cursor-related allocations
- Uses PostgreSQL's portal mechanism for efficient streaming of large result sets
- Operates within a subtransaction to provide proper exception handling and cleanup
- Validates query string encoding using pg_verifymbstr() before preparation
- The portal is pinned to prevent automatic cleanup and must be explicitly unpinned when the cursor is closed
- Sets up result tuple conversion infrastructure using PLy_input_setup_func() for RECORDOID type
- Returns NULL on any error, with appropriate error handling through the subtransaction mechanism
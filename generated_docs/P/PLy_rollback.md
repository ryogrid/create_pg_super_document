# PLy_rollback

## Location
src/pl/plpython/plpy_spi.c: 497 - 568

## Overview
PLy_rollback is a Python-callable function that rolls back the current transaction within PL/Python stored procedures, providing Python code access to PostgreSQL's transaction abort functionality.

## Definition
PyObject *PLy_rollback(PyObject *self, PyObject *args)

## Detailed Description
PLy_rollback implements transaction rollback functionality for PL/Python by wrapping PostgreSQL's SPI_rollback() function. Like PLy_commit, it operates within a PG_TRY/PG_CATCH block to handle any errors that may occur during the rollback operation. Upon successful rollback, the function resets the execution context's scratch memory context pointer since transaction boundaries clear this context. If an error occurs during rollback, the function captures the error data, converts it to an appropriate Python exception, and raises it in the Python environment.

## Parameters / Member Variables
- self: Standard Python method self parameter (unused in this static function)
- args: Python arguments tuple (unused - this function takes no arguments)

## Dependencies
- Functions called/Symbols referenced:
  - PLy_current_execution_context: Gets current PL/Python execution context
  - SPI_rollback: PostgreSQL SPI function to rollback current transaction
  - CopyErrorData: Copies error information for exception handling
  - FlushErrorState: Clears the current error state
  - hash_search: Searches for appropriate Python exception based on SQL error code
  - PLy_spi_exception_set: Sets up Python exception with PostgreSQL error data
  - FreeErrorData: Releases error data memory
- Called from (representative examples):
  - Referenced in plpy_spi.h header file for external access

## Notes and Other Information
- Shares identical error handling logic with PLy_commit, differing only in the core SPI operation (SPI_rollback vs SPI_commit)
- The function resets exec_ctx->scratch_ctx to NULL after rollback since transaction boundaries automatically clear scratch memory contexts
- Uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to properly handle rollback failures
- Maps PostgreSQL error codes to appropriate Python exceptions through PLy_spi_exceptions hash table
- Falls back to SPIError for custom/unknown error codes
- Returns Py_None on successful rollback, NULL on error (which triggers Python exception)
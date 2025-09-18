# PLy_commit

## Location
src/pl/plpython/plpy_spi.c: 450 - 496

## Overview
PLy_commit is a Python-callable function that commits the current transaction within PL/Python stored procedures, providing Python code access to PostgreSQL's transaction control functionality.

## Definition
PyObject *PLy_commit(PyObject *self, PyObject *args)

## Detailed Description
PLy_commit implements transaction commit functionality for PL/Python by wrapping PostgreSQL's SPI_commit() function. The function operates within a PG_TRY/PG_CATCH block to handle any errors that may occur during the commit operation. Upon successful commit, the function resets the execution context's scratch memory context pointer since transaction boundaries clear this context. If an error occurs during commit, the function captures the error data, converts it to an appropriate Python exception, and raises it in the Python environment.

## Parameters / Member Variables
- self: Standard Python method self parameter (unused in this static function)
- args: Python arguments tuple (unused - this function takes no arguments)

## Dependencies
- Functions called/Symbols referenced:
  - PLy_current_execution_context: Gets current PL/Python execution context
  - SPI_commit: PostgreSQL SPI function to commit current transaction
  - CopyErrorData: Copies error information for exception handling
  - FlushErrorState: Clears the current error state
  - hash_search: Searches for appropriate Python exception based on SQL error code
  - PLy_spi_exception_set: Sets up Python exception with PostgreSQL error data
  - FreeErrorData: Releases error data memory
- Called from (representative examples):
  - Referenced in plpy_spi.h header file for external access

## Notes and Other Information
- The function resets exec_ctx->scratch_ctx to NULL after commit since transaction boundaries automatically clear scratch memory contexts
- Uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to properly handle commit failures
- Maps PostgreSQL error codes to appropriate Python exceptions through PLy_spi_exceptions hash table
- Falls back to SPIError for custom/unknown error codes
- Returns Py_None on successful commit, NULL on error (which triggers Python exception)
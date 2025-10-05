# PLy_rollback

## Location
[src/pl/plpython/plpy_spi.c:497-568](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_spi.c#L497-L568)

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
  - [PLy_current_execution_context](PLy_current_execution_context.md): Gets current PL/Python execution context
  - [SPI_rollback](../S/SPI_rollback.md): PostgreSQL SPI function to rollback current transaction
  - [CopyErrorData](../C/CopyErrorData.md): Copies error information for exception handling
  - [FlushErrorState](../F/FlushErrorState.md): Clears the current error state
  - [hash_search](../h/hash_search.md): Searches for appropriate Python exception based on SQL error code
  - [PLy_spi_exception_set](PLy_spi_exception_set.md): Sets up Python exception with PostgreSQL error data
  - [FreeErrorData](../F/FreeErrorData.md): Releases error data memory
- Called from (representative examples):
  - Referenced in plpy_spi.h header file for external access

## Notes and Other Information
- Shares identical error handling logic with PLy_commit, differing only in the core SPI operation (SPI_rollback vs SPI_commit)
- The function resets exec_ctx->scratch_ctx to NULL after rollback since transaction boundaries automatically clear scratch memory contexts
- Uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to properly handle rollback failures
- Maps PostgreSQL error codes to appropriate Python exceptions through PLy_spi_exceptions hash table
- Falls back to SPIError for custom/unknown error codes
- Returns Py_None on successful rollback, NULL on error (which triggers Python exception)

## Simplified Source

```c
PyObject *
PLy_rollback(PyObject *self, PyObject *args)
{
	MemoryContext oldcontext = CurrentMemoryContext;
	PLyExecutionContext *exec_ctx = PLy_current_execution_context();

	// Attempt to rollback the transaction
	PG_TRY();
	{
		SPI_rollback();
		exec_ctx->scratch_ctx = NULL;  // Reset after transaction end
	}
	PG_CATCH();
	{
		// Handle rollback errors
		ErrorData *edata;
		PLyExceptionEntry *entry;
		PyObject *exc;

		// Capture error details
		MemoryContextSwitchTo(oldcontext);
		edata = CopyErrorData();
		FlushErrorState();
		exec_ctx->scratch_ctx = NULL;

		// Find appropriate Python exception
		entry = hash_search(PLy_spi_exceptions, &(edata->sqlerrcode), HASH_FIND, NULL);
		exc = entry ? entry->exc : PLy_exc_spi_error;

		// Raise Python exception
		PLy_spi_exception_set(exc, edata);
		FreeErrorData(edata);
		return NULL;
	}
	PG_END_TRY();

	Py_RETURN_NONE;
}
```
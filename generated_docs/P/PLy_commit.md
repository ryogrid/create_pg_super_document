# PLy_commit

## Location
[src/pl/plpython/plpy_spi.c:450-496](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_spi.c#L450-L496)

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
  - [PLy_current_execution_context](PLy_current_execution_context.md): Gets current PL/Python execution context
  - [SPI_commit](../S/SPI_commit.md): PostgreSQL SPI function to commit current transaction
  - [CopyErrorData](../C/CopyErrorData.md): Copies error information for exception handling
  - [FlushErrorState](../F/FlushErrorState.md): Clears the current error state
  - [hash_search](../h/hash_search.md): Searches for appropriate Python exception based on SQL error code
  - [PLy_spi_exception_set](PLy_spi_exception_set.md): Sets up Python exception with PostgreSQL error data
  - [FreeErrorData](../F/FreeErrorData.md): Releases error data memory
- Called from (representative examples):
  - Referenced in plpy_spi.h header file for external access

## Notes and Other Information
- The function resets exec_ctx->scratch_ctx to NULL after commit since transaction boundaries automatically clear scratch memory contexts
- Uses PostgreSQL's exception handling mechanism (PG_TRY/PG_CATCH) to properly handle commit failures
- Maps PostgreSQL error codes to appropriate Python exceptions through PLy_spi_exceptions hash table
- Falls back to SPIError for custom/unknown error codes
- Returns Py_None on successful commit, NULL on error (which triggers Python exception)

## Simplified Source

```c
PyObject *
PLy_commit(PyObject *self, PyObject *args)
{
	MemoryContext oldcontext = CurrentMemoryContext;
	PLyExecutionContext *exec_ctx = PLy_current_execution_context();

	// Attempt to commit the transaction
	PG_TRY();
	{
		SPI_commit();
		exec_ctx->scratch_ctx = NULL;  // Reset after transaction end
	}
	PG_CATCH();
	{
		// Handle commit errors
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
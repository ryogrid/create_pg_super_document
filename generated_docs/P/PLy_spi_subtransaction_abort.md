# PLy_spi_subtransaction_abort

## Location
src/pl/plpython/plpy_spi.c: 586 - 620

## Overview
PLy_spi_subtransaction_abort handles the error recovery path for subtransactions within PL/Python by rolling back the subtransaction and converting PostgreSQL errors into appropriate Python exceptions.

## Definition
void PLy_spi_subtransaction_abort(MemoryContext oldcontext, ResourceOwner oldowner)

## Detailed Description
PLy_spi_subtransaction_abort is called when an error occurs during SPI operations within a subtransaction. It first captures the current error information, then rolls back and releases the subtransaction using RollbackAndReleaseCurrentSubTransaction(). After restoring the original memory context and resource owner, it maps the PostgreSQL error code to an appropriate Python exception type and sets up the Python exception with the captured error data. This function ensures that PostgreSQL errors within subtransactions are properly propagated to Python code as exceptions while maintaining transactional consistency.

## Parameters / Member Variables
- oldcontext: The original memory context to restore after aborting the subtransaction
- oldowner: The original resource owner to restore after aborting the subtransaction

## Dependencies
- Functions called/Symbols referenced:
  - CopyErrorData: Copies the current error information for later use
  - FlushErrorState: Clears the PostgreSQL error state
  - RollbackAndReleaseCurrentSubTransaction: Rolls back and releases the current subtransaction
  - hash_search: Searches for appropriate Python exception based on SQL error code
  - PLy_spi_exception_set: Sets up Python exception with PostgreSQL error data
  - FreeErrorData: Releases the copied error data memory
- Called from (representative examples):
  - PLy_cursor_query: In PG_CATCH blocks for cursor query error handling
  - PLy_cursor_plan: In PG_CATCH blocks for cursor plan error handling
  - PLy_spi_prepare: In PG_CATCH blocks for SPI preparation error handling
  - PLy_spi_execute_plan: In PG_CATCH blocks for plan execution error handling
  - PLy_spi_execute_query: In PG_CATCH blocks for query execution error handling

## Notes and Other Information
- Must be paired with a preceding PLy_spi_subtransaction_begin call
- Should only be called in PG_CATCH blocks when errors occur during subtransaction operations
- Performs complete cleanup: rolls back subtransaction, restores context/owner, and sets Python exception
- Maps PostgreSQL error codes to specific Python exception types through PLy_spi_exceptions hash table
- Falls back to SPIError for custom or unknown error codes
- Unlike the commit function, this function sets up Python exceptions to propagate errors to Python code
- Part of the three-function subtransaction management suite (begin/commit/abort)
- The subtransaction rollback ensures that any changes made within the subtransaction are discarded
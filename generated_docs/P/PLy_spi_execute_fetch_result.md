# PLy_spi_execute_fetch_result

## Location
[src/pl/plpython/plpy_spi.c:340-449](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_spi.c#L340-L449)

## Overview
PLy_spi_execute_fetch_result processes SPI execution results and converts them into a Python-accessible PLyResultObject containing status information, row count, and converted tuple data.

## Definition


## Detailed Description
This static function is responsible for converting PostgreSQL SPI execution results into Python objects that can be returned to PL/Python code. It creates a PLyResultObject that encapsulates the execution status, number of affected/returned rows, and the actual result data converted to Python objects.

For SELECT queries that return data, the function converts each tuple in the result set to a Python dictionary using PLy_input_from_tuple. It handles large result sets by checking against Python's list size limits and creates appropriate error messages when limits are exceeded. The function manages memory contexts carefully, using a temporary context for conversion operations and ensuring the tuple descriptor is preserved in TopMemoryContext for later metadata access.

The function handles different types of SQL operations appropriately - for non-SELECT operations (INSERT, UPDATE, DELETE), it only stores the row count and status without processing tuple data.

## Parameters / Member Variables
- : SPI tuple table containing result tuples and metadata (can be NULL for non-SELECT operations)
- : Number of rows affected or returned by the query
- : SPI execution status code indicating success/failure and operation type

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_current_execution_context](PLy_current_execution_context.md): Gets current execution context
  - [PLy_result_new](PLy_result_new.md): Creates new PLyResultObject instance
  - [SPI_freetuptable](../S/SPI_freetuptable.md): Frees SPI tuple table resources
  - AllocSetContextCreate: Creates temporary memory context for conversions
  - [PLy_input_setup_func](PLy_input_setup_func.md)/PLy_input_setup_tuple: Sets up tuple-to-Python conversion
  - [PLy_input_from_tuple](PLy_input_from_tuple.md): Converts individual tuples to Python dictionaries
  - [CreateTupleDescCopy](../C/CreateTupleDescCopy.md): Creates persistent copy of tuple descriptor
  - [MemoryContextDelete](../M/MemoryContextDelete.md): Cleans up temporary memory context
- Called from (representative examples):
  - [PLy_spi_execute_plan](PLy_spi_execute_plan.md): After executing prepared plans
  - [PLy_spi_execute_query](PLy_spi_execute_query.md): After executing direct SQL queries

## Notes and Other Information
- This is a static function used internally within the plpy_spi.c module
- Handles both SELECT queries (with tuple data) and non-SELECT queries (row count only)
- Enforces Python's PY_SSIZE_T_MAX limit on result set size to prevent overflow
- Uses a temporary memory context during conversion to ensure proper cleanup on errors
- Preserves tuple descriptor in TopMemoryContext for metadata functions like result.colnames()
- Converts PostgreSQL tuples to Python dictionaries where column names are keys
- Properly manages Python reference counting for all created objects
- Handles errors gracefully with proper cleanup of partially constructed results
- The returned PLyResultObject provides attributes like status, nrows(), and rows for Python access
- Memory management follows PostgreSQL patterns with temporary contexts for intermediate data
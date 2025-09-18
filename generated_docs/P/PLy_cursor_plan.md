# PLy_cursor_plan

## Location
src/pl/plpython/plpy_cursorobject.c: 141 - 276

## Overview
Creates a PL/Python cursor object from a prepared plan object and optional parameter arguments for parameterized query execution.

## Definition


## Detailed Description
PLy_cursor_plan creates a cursor object for executing prepared SQL plans with parameters. This function is more complex than PLy_cursor_query as it must handle parameter validation, conversion, and binding. It validates that the number of provided arguments matches the plan's expected parameter count, converts Python objects to PostgreSQL Datum values, and opens a cursor using the prepared plan.

The function performs comprehensive argument validation, ensuring the second parameter is a sequence (if provided) and that the argument count matches the plan's requirements. It creates a temporary memory context for parameter conversion to ensure proper cleanup, converts each Python argument to the appropriate PostgreSQL type using PLy_output_convert(), and binds these values when opening the cursor portal.

The entire operation is wrapped in a subtransaction for proper error handling and resource management, with careful cleanup of both the cursor object and temporary contexts in case of failure.

## Parameters / Member Variables
- : PLyPlanObject containing the prepared plan to execute
- : Python sequence containing parameter values for the prepared plan (optional)

## Dependencies
- Functions called/Symbols referenced:
  - PySequence_Check (Python C API)
  - PyUnicode_Check (Python C API)
  - [PLy_exception_set](PLy_exception_set.md)
  - PySequence_Length (Python C API)
  - PLy_elog
  - [PLyUnicode_AsString](PLyUnicode_AsString.md)
  - [PLy_exception_set_plural](PLy_exception_set_plural.md)
  - PyObject_New (Python C API)
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - AllocSetContextCreate
  - [PLy_input_setup_func](PLy_input_setup_func.md)
  - [PLy_spi_subtransaction_begin](PLy_spi_subtransaction_begin.md)
  - [MemoryContextSwitchTo](../M/MemoryContextSwitchTo.md)
  - [palloc](../p/palloc.md)
  - PySequence_GetItem (Python C API)
  - [PLy_output_convert](PLy_output_convert.md)
  - [SPI_cursor_open](../S/SPI_cursor_open.md)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md)
  - [PinPortal](PinPortal.md)
  - [MemoryContextDelete](../M/MemoryContextDelete.md)
  - [PLy_spi_subtransaction_commit](PLy_spi_subtransaction_commit.md)
  - [PLy_spi_subtransaction_abort](PLy_spi_subtransaction_abort.md)
- Called from (representative examples):
  - [PLy_cursor](PLy_cursor.md)
  - [PLy_plan_cursor](PLy_plan_cursor.md)

## Notes and Other Information
- Validates argument types and counts before proceeding with cursor creation
- Uses a temporary memory context for parameter conversion to ensure proper cleanup
- Supports parameterized queries through proper Datum conversion and null handling
- Implements comprehensive error handling with automatic cleanup of allocated resources
- Creates the same cursor infrastructure as PLy_cursor_query but with parameter binding
- Uses PLy_exception_set_plural() for proper singular/plural error message formatting
- The temporary context is automatically cleaned up during subtransaction abort if an error occurs
- Parameter conversion uses PLy_output_convert() which handles type conversion from Python to PostgreSQL types
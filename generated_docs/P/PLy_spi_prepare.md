# PLy_spi_prepare

## Location
[src/pl/plpython/plpy_spi.c:39-153](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_spi.c#L39-L153)

## Overview
PLy_spi_prepare is a PL/Python function that prepares a SQL query for later execution, allowing for parameterized queries with type-safe parameter binding.

## Definition

```c
PyObject *
PLy_spi_prepare(PyObject *self, PyObject *args)
```
## Detailed Description
This function implements the  interface in PL/Python, enabling users to prepare SQL statements with optional parameter placeholders. It creates a PLyPlanObject that encapsulates the prepared statement, parameter types, and execution context. The function performs type validation for parameters, creates a dedicated memory context for the plan, and uses PostgreSQL's SPI (Server Programming Interface) to prepare the query for efficient repeated execution.

The prepared plan is stored in the top memory context to persist beyond the current function call, making it reusable across multiple executions. The function handles both parameterized and non-parameterized queries.

## Parameters / Member Variables
- : The Python module object (unused in this context)
- : Python tuple containing:
  -  (string): The SQL query to prepare, may contain parameter placeholders like , , etc.
  -  (optional sequence): List of parameter type names as strings (e.g., ["text", "int4"])

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_current_execution_context](PLy_current_execution_context.md): Gets current execution context
  - [PLy_exception_set](PLy_exception_set.md): Sets Python exceptions
  - [PLy_plan_new](PLy_plan_new.md): Creates new plan object
  - AllocSetContextCreate: Creates memory context for the plan
  - [PLy_spi_subtransaction_begin](PLy_spi_subtransaction_begin.md)/commit/abort: Manages subtransactions
  - [parseTypeString](../p/parseTypeString.md): Parses type names to OIDs
  - [PLy_output_setup_func](PLy_output_setup_func.md): Sets up parameter conversion functions
  - [SPI_prepare](../S/SPI_prepare.md): PostgreSQL SPI function to prepare the query
  - [SPI_keepplan](../S/SPI_keepplan.md): Transfers plan to top memory context
- Called from (representative examples):
  - Python code via plpy.prepare() interface

## Notes and Other Information
- Creates a dedicated memory context for each prepared plan to manage memory lifecycle
- Uses PostgreSQL's subtransaction mechanism to handle errors gracefully
- Validates that parameter types are provided as strings in a sequence
- The prepared plan persists beyond the current function execution through SPI_keepplan
- Supports both queries with and without parameters
- Error handling includes proper cleanup of Python objects and memory contexts
- The function is exposed to Python as the plpy.prepare() method
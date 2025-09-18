# PLy_cursor_fetch

## Location
src/pl/plpython/plpy_cursorobject.c: 366 - 466

## Overview
Fetches a specified number of rows from a PostgreSQL cursor and returns them as a PLyResultObject containing a Python list of row data.

## Definition
```c
static PyObject *PLy_cursor_fetch(PyObject *self, PyObject *args)
```

## Detailed Description
PLy_cursor_fetch is a static function that implements the "fetch" method for PLyCursor objects in PostgreSQL's PL/Python extension. It accepts a count parameter specifying how many rows to fetch from the cursor and returns a PLyResultObject containing the fetched rows as a Python list. The function performs extensive validation, including checking that the cursor is not closed and that the associated portal remains valid.

The function uses PostgreSQL's SPI (Server Programming Interface) to perform the actual cursor fetch operation within a subtransaction for safety. It handles the conversion of PostgreSQL tuple data to Python objects, with special consideration for Python list size limitations. The returned PLyResultObject includes status information, row count, and the actual row data formatted as Python objects.

## Parameters / Member Variables
- `self`: PyObject pointer to the PLyCursorObject instance
- `args`: Python tuple containing the fetch count parameter (parsed as integer)

## Dependencies
- Functions called/Symbols referenced:
  - PyArg_ParseTuple
  - PLy_current_execution_context
  - PLy_exception_set
  - GetPortalByName
  - PortalIsValid
  - PLy_result_new
  - PLy_spi_subtransaction_begin
  - SPI_cursor_fetch
  - PyLong_FromLong
  - PyLong_FromUnsignedLongLong
  - PyList_New
  - PLy_input_setup_tuple
  - PLy_input_from_tuple
  - PyList_SetItem
  - SPI_freetuptable
  - PLy_spi_subtransaction_commit
  - PLy_spi_subtransaction_abort
- Called from (representative examples):
  - Registered as "fetch" method in PLy_cursor_methods array

## Notes and Other Information
- Enforces Python list size limitations (PY_SSIZE_T_MAX) for large result sets
- Returns a PLyResultObject with status SPI_OK_FETCH and row count information
- Uses subtransactions for transactional safety during fetch operations
- Memory management includes proper reference counting for Python objects
- Error handling covers closed cursors and invalid portals
- The function is exposed to Python as the "fetch" method on cursor objects
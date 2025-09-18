# PLy_cursor_close

## Location
src/pl/plpython/plpy_cursorobject.c: 467 - 488

## Overview
Closes a PostgreSQL cursor by unpinning and closing the associated portal, marking the PLyCursor object as closed to prevent further operations.

## Definition
```c
static PyObject *PLy_cursor_close(PyObject *self, PyObject *unused)
```

## Detailed Description
PLy_cursor_close is a static function that implements the "close" method for PLyCursor objects in PostgreSQL's PL/Python extension. It provides a way for Python code to explicitly close a database cursor, releasing associated resources including the portal. The function first checks if the cursor is already closed to avoid double-closing operations.

When closing an active cursor, the function validates that the associated portal is still valid, then unpins it and calls SPI_cursor_close to perform the actual closure. After successful closure, it marks the cursor object as closed by setting the closed flag to true. This prevents subsequent operations on the cursor from succeeding, maintaining consistent state.

## Parameters / Member Variables
- `self`: PyObject pointer to the PLyCursorObject instance to close
- `unused`: Unused PyObject parameter (required by Python C API method signature)

## Dependencies
- Functions called/Symbols referenced:
  - GetPortalByName
  - PortalIsValid
  - PLy_exception_set
  - UnpinPortal
  - SPI_cursor_close
- Called from (representative examples):
  - Registered as "close" method in PLy_cursor_methods array

## Notes and Other Information
- Safe to call multiple times - subsequent calls have no effect if already closed
- Validates portal existence before attempting to close
- Uses UnpinPortal followed by SPI_cursor_close for proper resource cleanup
- Returns Py_None to Python indicating successful completion
- Sets cursor->closed flag to prevent further operations after closure
- The function is exposed to Python as the "close" method on cursor objects
- Error handling covers cases where cursor is in an aborted subtransaction
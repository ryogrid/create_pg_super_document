# PLy_cursor_iternext

## Location
src/pl/plpython/plpy_cursorobject.c: 304 - 365

## Overview
Implements the Python iterator protocol for PLyCursor objects, allowing cursors to be used in Python for-loops by fetching the next row from the cursor.

## Definition


## Detailed Description
PLy_cursor_iternext is a static function that serves as the iterator's next method for PLyCursor objects in PostgreSQL's PL/Python extension. It implements Python's iterator protocol by fetching exactly one row from the database cursor each time it's called. When no more rows are available, it raises a Python StopIteration exception to signal the end of iteration, following standard Python iterator behavior.

The function performs comprehensive error checking, including validation that the cursor is not closed and that the associated portal is still valid. It uses PostgreSQL's subtransaction mechanism to ensure transactional safety during the fetch operation. The fetched row is converted from PostgreSQL's internal tuple format to a Python object using the PLy input conversion system.

## Parameters / Member Variables
- : PyObject pointer to the PLyCursorObject instance being iterated

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_current_execution_context](PLy_current_execution_context.md)
  - [PLy_exception_set](PLy_exception_set.md)
  - GetPortalByName
  - PortalIsValid
  - [PLy_spi_subtransaction_begin](PLy_spi_subtransaction_begin.md)
  - [SPI_cursor_fetch](../S/SPI_cursor_fetch.md)
  - [PLy_input_setup_tuple](PLy_input_setup_tuple.md)
  - [PLy_input_from_tuple](PLy_input_from_tuple.md)
  - [SPI_freetuptable](../S/SPI_freetuptable.md)
  - [PLy_spi_subtransaction_commit](PLy_spi_subtransaction_commit.md)
  - [PLy_spi_subtransaction_abort](PLy_spi_subtransaction_abort.md)
- Called from (representative examples):
  - Registered as tp_iternext in PLy_CursorType Python type definition

## Notes and Other Information
- This function enables Python's for-loop syntax with PLyCursor objects
- Always fetches exactly one row per call (forward direction only)
- Returns NULL and sets PyExc_StopIteration when no more rows are available
- Uses PostgreSQL's subtransaction system for error recovery
- Memory context and resource owner are carefully managed during execution
- The function is registered in the PLy_CursorType structure as the tp_iternext method
# PLyCursorObject

## Location
src/pl/plpython/plpy_cursorobject.h: 11 - 18

## Overview
PLyCursorObject is a structure representing a PostgreSQL cursor object within the PL/Python procedural language extension, providing Python access to database cursors for efficient row-by-row processing of query results.

## Definition


## Detailed Description
PLyCursorObject is the core data structure that enables PL/Python functions to work with PostgreSQL cursors. It wraps a PostgreSQL portal (which implements cursors) and provides the necessary infrastructure for converting PostgreSQL data types to Python objects. This structure is designed as a Python object (inheriting from PyObject_HEAD) so it can be directly manipulated within Python code executed by PL/Python functions.

The structure maintains state information about the cursor including its portal name, conversion functions for result data, closure status, and its own memory context for proper resource management. When a cursor is created through PL/Python's cursor() function, it creates a PLyCursorObject instance that manages the underlying PostgreSQL portal and handles data type conversions between PostgreSQL and Python.

## Parameters / Member Variables
- : Standard Python object header that makes this structure a proper Python object
- : Name of the underlying PostgreSQL portal that implements the cursor functionality  
- : PLyDatumToOb structure containing conversion functions and metadata for transforming PostgreSQL Datum values to Python objects
- : Boolean flag indicating whether the cursor has been explicitly closed
- : Dedicated memory context for this cursor object, ensuring proper memory management and cleanup

## Dependencies
- Functions called/Symbols referenced:
  - [PLyDatumToOb](PLyDatumToOb.md)
- Called from (representative examples):
  - [PLy_cursor_query](PLy_cursor_query.md)
  - [PLy_cursor_plan](PLy_cursor_plan.md)  
  - [PLy_cursor_dealloc](PLy_cursor_dealloc.md)
  - [PLy_cursor_iternext](PLy_cursor_iternext.md)
  - [PLy_cursor_fetch](PLy_cursor_fetch.md)
  - [PLy_cursor_close](PLy_cursor_close.md)

## Notes and Other Information
- [PLyCursorObject](PLyCursorObject.md) implements Python's iterator protocol, allowing cursors to be used in Python for loops
- The structure uses a dedicated memory context (mcxt) to ensure proper cleanup of cursor-related memory allocations
- Cursors created through this structure support both manual fetching and automatic iteration
- The portalname field connects this Python object to the underlying PostgreSQL portal system
- Proper resource management is critical - cursors should be explicitly closed or will be cleaned up during transaction end
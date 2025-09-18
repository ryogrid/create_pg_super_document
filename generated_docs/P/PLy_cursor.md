# PLy_cursor

## Location
src/pl/plpython/plpy_cursorobject.c: 58 - 77

## Overview
Main entry point function for creating PL/Python cursor objects from either SQL query strings or prepared plan objects.

## Definition


## Detailed Description
PLy_cursor serves as the primary interface for the plpy.cursor() function in PL/Python. It acts as a dispatcher that accepts two different argument patterns: either a simple SQL query string, or a prepared plan object with optional parameters. The function uses Python's argument parsing to determine which type of cursor creation is requested and delegates to the appropriate specialized function.

When called with a string argument, it creates a cursor for a simple SQL query via PLy_cursor_query(). When called with a plan object (and optional arguments), it creates a cursor for a prepared statement via PLy_cursor_plan(). This design allows the same Python interface (plpy.cursor) to handle both use cases seamlessly.

## Parameters / Member Variables
- : Standard Python method self parameter (not used in this static function)
- : Python tuple containing the arguments passed to plpy.cursor() - either (query_string) or (plan_object, [parameters])

## Dependencies
- Functions called/Symbols referenced:
  - PyArg_ParseTuple (Python C API)
  - PLy_cursor_query
  - PLy_cursor_plan
  - PyErr_Clear (Python C API)
  - PLy_exception_set
- Called from (representative examples):
  - Not directly referenced (likely exposed through Python module interface)

## Notes and Other Information
- This function implements the overloaded behavior of plpy.cursor() in PL/Python
- Uses Python's argument parsing to distinguish between query string and plan object usage patterns
- Error handling clears Python exceptions between argument parsing attempts to try both patterns
- Returns NULL and sets a Python exception if neither argument pattern matches
- Part of the broader PL/Python cursor system that enables efficient iteration over query results
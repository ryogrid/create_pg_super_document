# PLy_cursor

## Location
[src/pl/plpython/plpy_cursorobject.c:58-77](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_cursorobject.c#L58-L77)

## Overview
Main entry point function for creating PL/Python cursor objects from either SQL query strings or prepared plan objects.

## Definition

```c
PyObject *
PLy_cursor(PyObject *self, PyObject *args)
```
## Detailed Description
PLy_cursor serves as the primary interface for the plpy.cursor() function in PL/Python. It acts as a dispatcher that accepts two different argument patterns: either a simple SQL query string, or a prepared plan object with optional parameters. The function uses Python's argument parsing to determine which type of cursor creation is requested and delegates to the appropriate specialized function.

When called with a string argument, it creates a cursor for a simple SQL query via PLy_cursor_query(). When called with a plan object (and optional arguments), it creates a cursor for a prepared statement via PLy_cursor_plan(). This design allows the same Python interface (plpy.cursor) to handle both use cases seamlessly.

## Parameters / Member Variables
- `*self`: Standard Python method self parameter (not used in this static function)
- `*args`: Python tuple containing the arguments passed to plpy.cursor() - either (query_string) or (plan_object, [parameters])
## Dependencies
- Functions called/Symbols referenced:
  - PyArg_ParseTuple (Python C API)
  - [PLy_cursor_query](PLy_cursor_query.md)
  - [PLy_cursor_plan](PLy_cursor_plan.md)
  - PyErr_Clear (Python C API)
  - [PLy_exception_set](PLy_exception_set.md)
- Called from (representative examples):
  - Not directly referenced (likely exposed through Python module interface)

## Notes and Other Information
- This function implements the overloaded behavior of plpy.cursor() in PL/Python
- Uses Python's argument parsing to distinguish between query string and plan object usage patterns
- Error handling clears Python exceptions between argument parsing attempts to try both patterns
- Returns NULL and sets a Python exception if neither argument pattern matches
- Part of the broader PL/Python cursor system that enables efficient iteration over query results

## Simplified Source

```c
PyObject *PLy_cursor(PyObject *self, PyObject *args) {
    char *query;
    PyObject *plan;
    PyObject *planargs = NULL;

    // Try parsing as query string first
    if (PyArg_ParseTuple(args, "s", &query))
        return PLy_cursor_query(query);

    PyErr_Clear();

    // Try parsing as plan object with optional arguments
    if (PyArg_ParseTuple(args, "O|O", &plan, &planargs))
        return PLy_cursor_plan(plan, planargs);

    // Neither pattern matched - raise error
    PLy_exception_set(PLy_exc_error, "plpy.cursor expected a query or a plan");
    return NULL;
}
```
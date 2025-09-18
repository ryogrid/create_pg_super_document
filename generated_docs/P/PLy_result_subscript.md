# PLy_result_subscript

## Location
[src/pl/plpython/plpy_resultobject.c:237-244](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_resultobject.c#L237-L244)

## Overview
PLy_result_subscript is a static function that implements the subscript operation (indexing) for PLyResultObject instances in the PL/Python extension, allowing Python code to access individual rows using bracket notation.

## Definition
```c
static PyObject *
PLy_result_subscript(PyObject *arg, PyObject *item)
```

## Detailed Description
This function enables subscript access to PLyResultObject instances, allowing Python code to retrieve specific rows from query results using indexing syntax like result[0] or result[key]. The function acts as a thin wrapper around PyObject_GetItem, delegating the actual indexing operation to the rows member of the result object.

This implementation allows PLyResultObject to behave like a Python sequence or mapping, providing intuitive access to query result data through standard Python indexing operations.

## Parameters / Member Variables
- `arg`: A PyObject pointer that should be cast to PLyResultObject, representing the result object being indexed
- `item`: A PyObject pointer representing the index or key used to access a specific row (typically an integer index)

## Dependencies
- Functions called/Symbols referenced:
  - [PLyResultObject](PLyResultObject.md) (structure type cast)
  - PyObject_GetItem (Python C API function)
- Called from (representative examples):
  - Used as mp_subscript method in PLyResultAsMapping mapping protocol (indirectly through Python's indexing mechanism)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the plpy_resultobject.c file
- The function delegates to PyObject_GetItem on the rows member, which should contain the actual row data as a Python sequence
- This enables natural Python syntax for accessing query results: result[0], result[1], etc.
- The function is typically assigned to the mp_subscript slot of the Python mapping protocol for PLyResultObject
- Error handling is delegated to PyObject_GetItem, which will raise appropriate Python exceptions for invalid indices
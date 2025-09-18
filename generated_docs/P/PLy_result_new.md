# PLy_result_new

## Location
src/pl/plpython/plpy_resultobject.c: 68 - 91

## Overview
Creates and initializes a new PLyResultObject instance that represents the results of a PostgreSQL query in PL/Python.

## Definition
```c
PyObject *PLy_result_new(void)
```

## Detailed Description
This function allocates and initializes a new PLyResultObject, which is the Python representation of query results in the PL/Python extension. It creates a new Python object of type PLy_ResultType and initializes all its fields to appropriate default values. The function handles proper Python reference counting and ensures the object is in a valid initial state. If memory allocation fails at any point, it properly cleans up and returns NULL.

## Parameters / Member Variables
This function takes no parameters and returns a PyObject pointer to the newly created PLyResultObject.

The initialized PLyResultObject contains:
- `nrows`: Set to -1 initially (unknown row count)
- `rows`: Empty Python list to hold result rows
- `status`: Set to Py_None initially (status unknown)
- `tupdesc`: Set to NULL initially (no tuple descriptor)

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_New() (Python C API)
  - Py_INCREF() (Python C API)
  - PyLong_FromLong() (Python C API)
  - PyList_New() (Python C API)
  - Py_DECREF() (Python C API)
  - [PLyResultObject](PLyResultObject.md) (struct type)
  - PLy_ResultType (type object)
- Called from (representative examples):
  - [PLy_cursor_fetch](PLy_cursor_fetch.md)() at src/pl/plpython/plpy_cursorobject.c:395
  - [PLy_spi_execute_fetch_result](PLy_spi_execute_fetch_result.md)() at src/pl/plpython/plpy_spi.c:346

## Notes and Other Information
- Returns NULL if memory allocation fails for either the object itself or the rows list
- Properly manages Python reference counts for all contained objects
- The returned object must eventually be freed using Python's reference counting (Py_DECREF)
- All fields are initialized to safe default values that indicate unset/unknown state
- This is a factory function that should be used whenever a new result object is needed
- The object is ready to be populated with actual query results after creation
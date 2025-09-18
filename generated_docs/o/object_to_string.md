# object_to_string

## Location
src/pl/plpython/plpy_plpymodule.c: 377 - 397

## Overview
A utility function that safely converts a Python object to a C string representation within PL/Python extension.

## Definition
static char *object_to_string(PyObject *obj)

## Detailed Description
This function provides a safe mechanism for converting Python objects to string representations in the PL/Python procedural language extension. It handles the conversion process by using Pythons built-in string conversion functionality and ensures proper memory management by creating a PostgreSQL-allocated copy of the resulting string.

The function serves as a bridge between Pythons object system and PostgreSQLs string handling requirements, particularly important for logging and error reporting functionality within PL/Python.

## Parameters / Member Variables
- obj: A PyObject pointer to the Python object that needs to be converted to string. Can be NULL.

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_Str (Python C API)
  - PLyUnicode_AsString (PL/Python utility)
  - pstrdup (PostgreSQL memory management)
  - Py_DECREF (Python C API)
- Called from (representative examples):
  - PLy_output (multiple times for various error message components)

## Notes and Other Information
- Returns NULL if the input object is NULL or if string conversion fails
- Uses pstrdup to ensure the returned string is allocated in PostgreSQL memory context
- Properly manages Python object reference counting by decrementing the temporary string object
- The returned string must be freed by the caller using pfree() when no longer needed
- This function is critical for error reporting and logging functionality in PL/Python procedures
# set_string_attr

## Location
src/pl/plpython/plpy_elog.c: 583 - 604

## Overview
A utility function that safely sets a string attribute on a Python object, handling NULL values gracefully by setting the attribute to Python's None.

## Definition


## Detailed Description
This function is a helper utility in PostgreSQL's PL/Python extension that sets a string attribute on a Python object. It handles the conversion from C strings to Python unicode objects and manages reference counting properly. The function gracefully handles NULL string values by setting the attribute to Python's None object instead. This is particularly useful when setting error details where some fields may not be available.

The function performs proper Python reference counting, incrementing the reference count for Py_None when needed and decrementing the reference count for the created value object before returning. It returns a boolean indicating whether the attribute setting operation was successful.

## Parameters / Member Variables
- : The Python object on which to set the attribute
- : The name of the attribute to set (as a C string)
- : The string value to set, or NULL to set the attribute to None

## Dependencies
- Functions called/Symbols referenced:
  - PLyUnicode_FromString
  - PyObject_SetAttrString (Python C API)
  - Py_INCREF (Python C API)
  - Py_DECREF (Python C API)
- Called from (representative examples):
  - PLy_exception_set_with_details (multiple times for setting various error attributes like sqlstate, detail, hint, query, schema_name, table_name, column_name, datatype_name, constraint_name)

## Notes and Other Information
- This is a static function within the PL/Python error logging module, designed specifically for setting string attributes on Python exception objects
- The function properly handles Python reference counting to prevent memory leaks
- It's primarily used when creating detailed Python exceptions from PostgreSQL ErrorData structures
- The function returns false if the PLyUnicode_FromString conversion fails or if PyObject_SetAttrString fails
- This function is crucial for providing detailed error information to Python code when PostgreSQL errors occur
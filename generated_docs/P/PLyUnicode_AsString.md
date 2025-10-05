# PLyUnicode_AsString

## Location
[src/pl/plpython/plpy_util.c:83-96](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_util.c#L83-L96)

## Overview
Converts a Python unicode object to a C string in PostgreSQL server encoding, returning a palloc'ed string suitable for PostgreSQL memory management.

## Definition
```c
char *PLyUnicode_AsString(PyObject *unicode)
```

## Detailed Description
PLyUnicode_AsString is a convenience wrapper function that converts Python Unicode objects to null-terminated C strings in PostgreSQL server encoding. It leverages PLyUnicode_Bytes to perform the encoding conversion, then extracts the string data and duplicates it using PostgreSQL's memory allocation system (palloc). Unlike PLyUnicode_Bytes which returns a Python bytes object, this function returns a C string that can be used directly with PostgreSQL's string handling functions.

The function ensures proper memory management by decrementing the reference count of the intermediate Python bytes object while returning a PostgreSQL-allocated copy of the string data.

## Parameters / Member Variables
- `unicode`: A Python Unicode object to be converted to a C string in server encoding

## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_Bytes](PLyUnicode_Bytes.md)
  - PyBytes_AsString (Python C API)
  - [pstrdup](../p/pstrdup.md)
  - Py_XDECREF (Python C API)
- Called from (representative examples):
  - [PLy_cursor_plan](PLy_cursor_plan.md)
  - [PLy_traceback](PLy_traceback.md) (multiple times)
  - [PLy_get_sqlerrcode](PLy_get_sqlerrcode.md)
  - [get_string_attr](../g/get_string_attr.md)
  - [PLy_exec_trigger](PLy_exec_trigger.md)
  - [PLy_modify_tuple](PLy_modify_tuple.md)
  - [object_to_string](../o/object_to_string.md)
  - [PLy_output](PLy_output.md) (multiple times)
  - [PLy_spi_prepare](PLy_spi_prepare.md)
  - [PLy_spi_execute_plan](PLy_spi_execute_plan.md)

## Notes and Other Information
- Returns a palloc'ed string that must be pfree'd by the caller
- No Python object references are passed out of this function - all Python objects are properly cleaned up
- Uses Py_XDECREF instead of Py_DECREF for safer handling of potentially NULL objects
- Widely used throughout the PL/Python codebase for converting Python strings to PostgreSQL-compatible C strings
- The returned string is in PostgreSQL server encoding, making it suitable for database operations

## Simplified Source

```c
char *PLyUnicode_AsString(PyObject *unicode) {
    // Convert Python unicode to bytes in server encoding
    PyObject *bytes_obj = PLyUnicode_Bytes(unicode);

    // Extract C string and create PostgreSQL-allocated copy
    char *result = pstrdup(PyBytes_AsString(bytes_obj));

    // Clean up temporary Python object
    Py_XDECREF(bytes_obj);

    return result;
}
```
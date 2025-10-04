# get_string_attr

## Location
[src/pl/plpython/plpy_elog.c:567-582](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_elog.c#L567-L582)

## Overview
Extracts a string attribute value from a Python object and converts it to a PostgreSQL-allocated C string for use in PL/Python error handling.

## Definition
```c
static void get_string_attr(PyObject *obj, char *attrname, char **str)
```

## Detailed Description
This function safely retrieves a named attribute from a Python object and converts it to a C string using PostgreSQL's memory allocation system. It handles the common case where the attribute may not exist or may be None by checking the returned value before conversion. The function uses PLyUnicode_AsString for proper Unicode handling and pstrdup to create a copy in PostgreSQL's memory context. This is essential for extracting error details from Python exception objects while maintaining proper memory management.

## Parameters / Member Variables
- `obj`: Python object from which to extract the attribute
- `attrname`: Name of the attribute to retrieve
- `str`: Pointer to char pointer where the result string will be stored

## Dependencies
- Functions called/Symbols referenced:
  - PyObject_GetAttrString (Python C API function)
  - [PLyUnicode_AsString](../P/PLyUnicode_AsString.md) (PL/Python Unicode conversion function)
  - [pstrdup](../p/pstrdup.md) (PostgreSQL memory allocation function)
  - Py_XDECREF (Python reference counting function)
- Called from (representative examples):
  - [PLy_get_error_data](../P/PLy_get_error_data.md) (multiple calls for different attributes)

## Notes and Other Information
- Safely handles cases where the attribute doesn't exist or is None
- Uses PostgreSQL's palloc memory management through pstrdup
- Properly manages Python reference counting with Py_XDECREF
- Part of the error information extraction system in PL/Python
- Static function used internally for processing Python exception objects
- Handles Unicode strings properly through PLyUnicode_AsString wrapper

## Simplified Source

```c
static void get_string_attr(PyObject *obj, char *attrname, char **str) {
    PyObject *val;

    // Get the named attribute from the Python object
    val = PyObject_GetAttrString(obj, attrname);

    // Convert to C string if attribute exists and is not None
    if (val != NULL && val != Py_None) {
        *str = pstrdup(PLyUnicode_AsString(val));
    }

    // Clean up Python object reference
    Py_XDECREF(val);
}
```
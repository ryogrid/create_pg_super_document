# PLyUnicode_FromStringAndSize

## Location
[src/pl/plpython/plpy_util.c:97-117](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_util.c#L97-L117)

## Overview
Converts a C string in PostgreSQL server encoding to a Python unicode object, handling encoding conversion to UTF-8 when necessary.

## Definition
```c
PyObject *PLyUnicode_FromStringAndSize(const char *s, Py_ssize_t size)
```

## Detailed Description
PLyUnicode_FromStringAndSize converts C strings from PostgreSQL server encoding to Python Unicode objects. The function first converts the input string from server encoding to UTF-8 using pg_server_to_any. If the database encoding is already UTF-8, no conversion is needed and the original string is used directly. Otherwise, the converted UTF-8 string is used to create the Python Unicode object and the temporary UTF-8 string is freed.

This function handles the size parameter explicitly, making it suitable for strings that may contain null bytes or when the exact length is known, unlike PLyUnicode_FromString which assumes null-terminated strings.

## Parameters / Member Variables
- `s`: A C string in PostgreSQL server encoding to be converted
- `size`: The length of the string in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [pg_server_to_any](../p/pg_server_to_any.md)
  - PG_UTF8
  - PyUnicode_FromStringAndSize (Python C API)
  - PyUnicode_FromString (Python C API)
  - [pfree](../p/pfree.md)
- Called from:
  - [PLyUnicode_FromString](PLyUnicode_FromString.md)

## Notes and Other Information
- Returns a new Python Unicode object with transferred reference ownership to the caller
- Efficiently handles the case where database encoding is UTF-8 by avoiding unnecessary conversion
- Uses PyUnicode_FromStringAndSize when no conversion is needed to preserve exact string length
- Uses PyUnicode_FromString for converted strings since pg_server_to_any returns null-terminated UTF-8
- Properly manages memory by freeing converted UTF-8 strings when they differ from the original input
- Essential for converting PostgreSQL string data to Python Unicode objects in the PL/Python interface
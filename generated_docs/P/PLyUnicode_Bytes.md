# PLyUnicode_Bytes

## Location
src/pl/plpython/plpy_util.c: 21 - 82

## Overview
Converts a Python unicode object to a Python bytes object encoded in PostgreSQL server encoding, handling encoding conversion from UTF-8 when necessary.

## Definition
```c
PyObject *PLyUnicode_Bytes(PyObject *unicode)
```

## Detailed Description
PLyUnicode_Bytes is a utility function in the PL/Python extension that converts Python Unicode objects to Python bytes objects using the PostgreSQL server's character encoding. The function first encodes the Unicode object to UTF-8, then converts it to the server encoding if the database encoding is not UTF-8. This two-step conversion approach is used because Python doesn't support all encodings that PostgreSQL does (specifically EUC_TW and MULE_INTERNAL), so UTF-8 serves as an intermediary format.

The function includes comprehensive error handling using PostgreSQL's exception system (PG_TRY/PG_CATCH) to ensure proper cleanup of Python objects in case of encoding conversion failures.

## Parameters / Member Variables
- `unicode`: A Python Unicode object to be converted to bytes in server encoding

## Dependencies
- Functions called/Symbols referenced:
  - PyUnicode_AsUTF8String (Python C API)
  - PyBytes_AsString (Python C API) 
  - PyBytes_FromStringAndSize (Python C API)
  - PLy_elog
  - [GetDatabaseEncoding](../G/GetDatabaseEncoding.md)
  - [pg_any_to_server](../p/pg_any_to_server.md)
  - PG_TRY/PG_CATCH/PG_END_TRY/PG_RE_THROW
  - PG_UTF8
- Called from:
  - [PLyObject_AsString](PLyObject_AsString.md) (multiple times)
  - [PLyUnicode_AsString](PLyUnicode_AsString.md)

## Notes and Other Information
- Uses UTF-8 as an intermediary encoding format to handle PostgreSQL encodings not supported by Python
- Implements proper memory management with Py_DECREF calls and pfree for allocated memory
- Returns a new Python bytes object with transferred reference ownership to the caller
- Error handling ensures Python reference counts are properly managed even when PostgreSQL exceptions occur
# PLyUnicode_Bytes

## Location
[src/pl/plpython/plpy_util.c:21-82](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_util.c#L21-L82)

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

## Simplified Source

```c
PyObject *
PLyUnicode_Bytes(PyObject *unicode)
{
    PyObject *bytes, *rv;
    char *utf8string, *encoded;

    // Convert Unicode to UTF-8 bytes first
    bytes = PyUnicode_AsUTF8String(unicode);
    if (bytes == NULL)
        PLy_elog(ERROR, "could not convert Python Unicode object to bytes");

    utf8string = PyBytes_AsString(bytes);
    if (utf8string == NULL) {
        Py_DECREF(bytes);
        PLy_elog(ERROR, "could not extract bytes from encoded string");
    }

    // Convert to server encoding if database is not UTF-8
    if (GetDatabaseEncoding() != PG_UTF8) {
        PG_TRY();
        {
            encoded = pg_any_to_server(utf8string, strlen(utf8string), PG_UTF8);
        }
        PG_CATCH();
        {
            Py_DECREF(bytes);
            PG_RE_THROW();
        }
        PG_END_TRY();
    } else {
        encoded = utf8string;
    }

    // Create final bytes object in server encoding
    rv = PyBytes_FromStringAndSize(encoded, strlen(encoded));

    // Clean up allocated memory
    if (utf8string != encoded)
        pfree(encoded);
    Py_DECREF(bytes);

    return rv;
}
```
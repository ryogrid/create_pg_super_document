# PLyObject_AsString

## Location
[src/pl/plpython/plpy_typeio.c:1024-1073](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L1024-L1073)

## Overview
Converts a Python object to a C string in PostgreSQL server encoding, with special handling for different Python object types and comprehensive validation.

## Definition

```c
char *
PLyObject_AsString(PyObject *plrv)
```
## Detailed Description
This function provides a robust conversion mechanism from Python objects to C strings encoded in PostgreSQL server encoding. It handles different Python object types with specialized logic:

For Unicode strings, it directly converts using PLyUnicode_Bytes. For float objects, it uses repr() instead of str() to preserve precision and avoid lossy conversions. For all other object types, it uses the standard str() method.

The function includes comprehensive validation to ensure the resulting string is safe for PostgreSQL use. It checks for embedded null bytes by comparing the reported Python bytes length with the actual C string length. It also validates that the resulting string is properly encoded using pg_verifymbstr.

The function is exported for use by add-on transform modules, making it a public interface for Python-to-string conversions within the PL/Python ecosystem.

## Parameters / Member Variables
- `*plrv`: Python object to convert to a C string
## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_Bytes](PLyUnicode_Bytes.md)
  - PyObject_Repr
  - PyObject_Str
  - PLy_elog
  - [pstrdup](../p/pstrdup.md)
  - [pg_verifymbstr](../p/pg_verifymbstr.md)
- Called from (representative examples):
  - [PLyObject_ToScalar](PLyObject_ToScalar.md)
  - [PLyUnicode_ToComposite](PLyUnicode_ToComposite.md)
  - [PLyObToDatum](PLyObToDatum.md)

## Notes and Other Information
The function implements important safety measures including null byte detection and multibyte string validation. The special handling of float objects using repr() instead of str() prevents precision loss that could occur with standard string conversion. The exported nature of this function makes it available to transform modules, indicating its role as a fundamental conversion utility in the PL/Python infrastructure.

## Simplified Source

```c
char *PLyObject_AsString(PyObject *plrv) {
    PyObject *plrv_bo;
    char *plrv_sc;
    size_t plen, slen;

    // Convert Python object to bytes based on type
    if (PyUnicode_Check(plrv)) {
        plrv_bo = PLyUnicode_Bytes(plrv);
    } else if (PyFloat_Check(plrv)) {
        // Use repr() for floats to avoid precision loss
        PyObject *s = PyObject_Repr(plrv);
        plrv_bo = PLyUnicode_Bytes(s);
        Py_XDECREF(s);
    } else {
        // Use str() for all other types
        PyObject *s = PyObject_Str(plrv);
        plrv_bo = PLyUnicode_Bytes(s);
        Py_XDECREF(s);
    }

    if (!plrv_bo)
        PLy_elog(ERROR, "could not create string representation of Python object");

    // Create C string and validate lengths
    plrv_sc = pstrdup(PyBytes_AsString(plrv_bo));
    plen = PyBytes_Size(plrv_bo);
    slen = strlen(plrv_sc);

    Py_XDECREF(plrv_bo);

    // Check for embedded null bytes and length consistency
    if (slen < plen)
        ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH),
                errmsg("could not convert Python object into cstring: "
                       "Python string representation appears to contain null bytes")));
    else if (slen > plen)
        elog(ERROR, "could not convert Python object into cstring: "
                   "Python string longer than reported length");

    // Validate multibyte encoding
    pg_verifymbstr(plrv_sc, slen, false);

    return plrv_sc;
}
```
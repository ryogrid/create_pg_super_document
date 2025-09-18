# PLyBytes_FromBytea

## Location
[src/pl/plpython/plpy_typeio.c:628-641](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L628-L641)

## Overview
Converts a PostgreSQL bytea (binary data) datum to a Python bytes object.

## Definition
```c
static PyObject *PLyBytes_FromBytea(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
PLyBytes_FromBytea is a conversion function within PostgreSQL's PL/Python extension that transforms a bytea datum into a Python bytes object. The function extracts the binary data from the datum using DatumGetByteaPP(), retrieves the data pointer and size using VARDATA_ANY() and VARSIZE_ANY_EXHDR() macros, and creates a Python bytes object using PyBytes_FromStringAndSize(). This function handles the conversion of PostgreSQL's variable-length binary data type to Python's bytes type, preserving the binary content exactly.

## Parameters / Member Variables
- `arg`: PLyDatumToOb pointer containing conversion context information (unused in this function)
- `d`: Datum containing the bytea value to be converted

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetByteaPP: Extracts bytea value from Datum with potential detoasting
  - VARDATA_ANY: Macro to get pointer to variable-length data content
  - VARSIZE_ANY_EXHDR: Macro to get size of variable-length data excluding header
  - PyBytes_FromStringAndSize: Creates Python bytes object from data pointer and size
- Called from (representative examples):
  - [PLy_input_setup_func](PLy_input_setup_func.md): Sets up input conversion functions for PostgreSQL to Python data conversion

## Notes and Other Information
- This is a static function within the PL/Python type conversion system
- Handles PostgreSQL's variable-length binary data format correctly by using appropriate macros
- The function preserves binary data integrity during conversion
- Returns a new Python bytes object reference that must be properly managed by the caller
- Uses detoasting-aware functions to handle potentially compressed/external bytea values
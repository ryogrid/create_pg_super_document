# PLyLong_FromOid

## Location
[src/pl/plpython/plpy_typeio.c:622-627](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L622-L627)

## Overview
Converts a PostgreSQL Oid (Object Identifier) datum to a Python long integer object.

## Definition

```c
static PyObject *
PLyLong_FromOid(PLyDatumToOb *arg, Datum d)
```
## Detailed Description
PLyLong_FromOid is a specialized conversion function within PostgreSQL's PL/Python extension that transforms an Oid datum into a Python long integer. The function extracts the Oid value from the datum using DatumGetObjectId() and converts it to an unsigned long Python object using PyLong_FromUnsignedLong(). This function is part of the type conversion system that bridges PostgreSQL's internal data types with Python objects.

## Parameters / Member Variables
- `*arg`: PLyDatumToOb pointer containing conversion context information (unused in this function)
- `d`: Datum containing the Oid value to be converted
## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetObjectId](../D/DatumGetObjectId.md): Extracts Oid value from Datum
  - PyLong_FromUnsignedLong: Creates Python long object from unsigned long
- Called from (representative examples):
  - [PLy_input_setup_func](PLy_input_setup_func.md): Sets up input conversion functions for PostgreSQL to Python data conversion

## Notes and Other Information
- This is a static function within the PL/Python type conversion system
- The function assumes the datum contains a valid Oid value
- Returns a new Python object reference that must be properly managed by the caller
- Part of the datum-to-Python object conversion infrastructure in PL/Python

## Simplified Source

```c
static PyObject *PLyLong_FromOid(PLyDatumToOb *arg, Datum d)
{
    // Convert PostgreSQL Oid datum to Python long integer
    return PyLong_FromUnsignedLong(DatumGetObjectId(d));
}
```
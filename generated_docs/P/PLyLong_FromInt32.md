# PLyLong_FromInt32

## Location
src/pl/plpython/plpy_typeio.c: 610 - 615

## Overview
Converts a PostgreSQL int4 (integer) value to a Python long object in the PL/Python extension.

## Definition
```c
static PyObject *
PLyLong_FromInt32(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
This function handles the conversion of PostgreSQL's int4 data type (also known as integer, a 32-bit signed integer) to Python's long object type. The function operates as a simple wrapper that extracts the 32-bit integer value from a PostgreSQL Datum and creates the corresponding Python long object using the Python C API.

The conversion process is straightforward, promoting the 32-bit integer to Python's arbitrary precision long type. This is PostgreSQL's most commonly used integer type, corresponding to the standard 'integer' or 'int' type in SQL DDL statements.

## Parameters / Member Variables
- `arg`: A pointer to PLyDatumToOb structure containing conversion context information (unused in this simple conversion)
- `d`: The PostgreSQL Datum containing the int4 (integer) value to be converted to a Python object

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt32](../D/DatumGetInt32.md): Extracts the 32-bit integer value from the PostgreSQL Datum
  - PyLong_FromLong: Python C API function to create a Python long object from a C long integer
- Called from (representative examples):
  - [PLy_input_setup_func](PLy_input_setup_func.md): Sets up input conversion functions for various PostgreSQL data types

## Notes and Other Information
- This is a static function, meaning it's only accessible within the plpy_typeio.c file
- Handles conversion from PostgreSQL's 32-bit signed integer range (-2,147,483,648 to 2,147,483,647) to Python's unlimited precision integers
- Part of a family of integer conversion functions (PLyLong_FromInt16, PLyLong_FromInt64) that handle different integer sizes
- The most commonly used integer conversion function since int4/integer is PostgreSQL's default integer type
- In Python 3, all integers are long objects internally, so this creates what appears as a regular integer in Python code
- No precision is lost in the conversion since Python's long type can represent any 32-bit integer value
- The function follows the standard PLyDatumToOb function pointer pattern used throughout PL/Python's type conversion framework
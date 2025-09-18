# PLyLong_FromInt16

## Location
src/pl/plpython/plpy_typeio.c: 604 - 609

## Overview
Converts a PostgreSQL int2 (smallint) value to a Python long object in the PL/Python extension.

## Definition
```c
static PyObject *
PLyLong_FromInt16(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
This function provides conversion functionality for PostgreSQL's int2 data type (also known as smallint, a 16-bit signed integer) to Python's long object type. The function serves as a straightforward wrapper that extracts the 16-bit integer value from a PostgreSQL Datum and creates the corresponding Python long object using the Python C API.

The conversion is direct and handles the type promotion from the smaller 16-bit integer to Python's arbitrary precision long type. In Python 3, all integers are long objects, so this function effectively creates a Python integer from the PostgreSQL smallint value.

## Parameters / Member Variables
- `arg`: A pointer to PLyDatumToOb structure containing conversion context information (unused in this simple conversion)
- `d`: The PostgreSQL Datum containing the int2 (smallint) value to be converted to a Python object

## Dependencies
- Functions called/Symbols referenced:
  - DatumGetInt16: Extracts the 16-bit integer value from the PostgreSQL Datum
  - PyLong_FromLong: Python C API function to create a Python long object from a C long integer
- Called from (representative examples):
  - PLy_input_setup_func: Sets up input conversion functions for various PostgreSQL data types

## Notes and Other Information
- This is a static function, meaning it's only accessible within the plpy_typeio.c file
- The function handles the conversion from PostgreSQL's 16-bit signed integer range (-32,768 to 32,767) to Python's unlimited precision integers
- Part of a family of similar conversion functions for different integer sizes (PLyLong_FromInt32, PLyLong_FromInt64)
- The conversion automatically handles the promotion from the smaller integer type to Python's long type without any precision loss
- In Python 3, the distinction between int and long was removed, so PyLong_FromLong creates what appears as a regular integer in Python code
- The function signature follows the standard PLyDatumToOb function pointer pattern used throughout the PL/Python type conversion system
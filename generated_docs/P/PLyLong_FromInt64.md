# PLyLong_FromInt64

## Location
src/pl/plpython/plpy_typeio.c: 616 - 621

## Overview
Converts a PostgreSQL int8 (bigint) value to a Python long object in the PL/Python extension.

## Definition
```c
static PyObject *
PLyLong_FromInt64(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
This function provides conversion functionality for PostgreSQL's int8 data type (also known as bigint, a 64-bit signed integer) to Python's long object type. The function serves as a wrapper that extracts the 64-bit integer value from a PostgreSQL Datum and creates the corresponding Python long object using the Python C API.

Unlike the smaller integer conversion functions, this function uses PyLong_FromLongLong() to handle the full 64-bit range, ensuring that large integer values are properly converted without overflow. This is particularly important for PostgreSQL's bigint type, which can store values ranging from -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807.

## Parameters / Member Variables
- `arg`: A pointer to PLyDatumToOb structure containing conversion context information (unused in this simple conversion)
- `d`: The PostgreSQL Datum containing the int8 (bigint) value to be converted to a Python object

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetInt64](../D/DatumGetInt64.md): Extracts the 64-bit integer value from the PostgreSQL Datum
  - PyLong_FromLongLong: Python C API function to create a Python long object from a C long long integer
- Called from (representative examples):
  - [PLy_input_setup_func](PLy_input_setup_func.md): Sets up input conversion functions for various PostgreSQL data types

## Notes and Other Information
- This is a static function, meaning it's only accessible within the plpy_typeio.c file
- Handles the full range of PostgreSQL's 64-bit signed integers without precision loss
- Uses PyLong_FromLongLong() instead of PyLong_FromLong() to properly handle the full 64-bit range on all platforms
- Part of the family of integer conversion functions (PLyLong_FromInt16, PLyLong_FromInt32) for different integer sizes
- Essential for applications that work with large numeric identifiers, timestamps, or other data requiring 64-bit integer precision
- In Python 3, all integers are long objects internally, so this creates what appears as a regular integer in Python code
- The function signature follows the standard PLyDatumToOb function pointer pattern used throughout PL/Python's type conversion system
- Critical for maintaining data integrity when working with PostgreSQL's bigint columns that contain values beyond the 32-bit integer range
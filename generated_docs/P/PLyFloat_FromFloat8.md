# PLyFloat_FromFloat8

## Location
[src/pl/plpython/plpy_typeio.c:564-569](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_typeio.c#L564-L569)

## Overview
Converts a PostgreSQL float8 (double precision) value to a Python float object in the PL/Python extension.

## Definition


## Detailed Description
This function serves as a conversion utility in PostgreSQL's PL/Python extension, specifically handling the transformation of PostgreSQL's float8 data type (which represents double precision floating-point numbers) into Python float objects. The function is a straightforward wrapper that extracts the float8 value from a PostgreSQL Datum and creates the corresponding Python object using the Python C API.

The function follows the standard PLy conversion pattern used throughout the PL/Python extension for type conversions, taking a PLyDatumToOb argument structure and a Datum containing the PostgreSQL value to be converted.

## Parameters / Member Variables
- : A pointer to PLyDatumToOb structure containing conversion context information (unused in this simple conversion)
- : The PostgreSQL Datum containing the float8 value to be converted to a Python object

## Dependencies
- Functions called/Symbols referenced:
  - [DatumGetFloat8](../D/DatumGetFloat8.md): Extracts the float8 value from the PostgreSQL Datum
  - PyFloat_FromDouble: Python C API function to create a Python float object from a C double
- Called from (representative examples):
  - [PLy_input_setup_func](PLy_input_setup_func.md): Sets up input conversion functions for various PostgreSQL data types

## Notes and Other Information
- This is a static function, meaning it's only accessible within the plpy_typeio.c file
- The function is part of the larger type conversion system in PL/Python that handles bidirectional data type mapping between PostgreSQL and Python
- The conversion is direct and doesn't require any special handling for null values or error cases, as those are typically handled at a higher level in the conversion framework
- The function signature follows the standard PLyDatumToOb function pointer pattern used for input conversions in PL/Python
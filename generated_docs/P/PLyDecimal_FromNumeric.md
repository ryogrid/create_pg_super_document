# PLyDecimal_FromNumeric

## Location
src/pl/plpython/plpy_typeio.c: 570 - 603

## Overview
Converts a PostgreSQL numeric value to a Python Decimal object in the PL/Python extension, providing high-precision decimal arithmetic support.

## Definition
```c
static PyObject *
PLyDecimal_FromNumeric(PLyDatumToOb *arg, Datum d)
```

## Detailed Description
This function handles the conversion of PostgreSQL's numeric data type (arbitrary precision decimal numbers) to Python's Decimal objects. The function implements a lazy initialization pattern for the decimal constructor, attempting to use the faster cdecimal module if available, falling back to the standard decimal module if cdecimal is not present.

The conversion process involves first converting the PostgreSQL numeric value to its string representation using the numeric_out function, then creating a Python Decimal object from that string. This approach preserves the full precision of the PostgreSQL numeric value without any loss of accuracy that might occur with floating-point conversions.

The function uses static storage for the decimal constructor to avoid repeatedly importing and looking up the Decimal class on subsequent calls, improving performance for repeated conversions.

## Parameters / Member Variables
- `arg`: A pointer to PLyDatumToOb structure containing conversion context information (unused in this conversion)
- `d`: The PostgreSQL Datum containing the numeric value to be converted to a Python Decimal object

## Dependencies
- Functions called/Symbols referenced:
  - PyImport_ImportModule: Python C API function to import cdecimal or decimal module
  - PyErr_Clear: Clears Python exception state when cdecimal import fails
  - PyObject_GetAttrString: Gets the Decimal attribute from the imported module
  - [numeric_out](../n/numeric_out.md): PostgreSQL function that converts numeric values to string representation
  - DirectFunctionCall1: PostgreSQL macro for calling functions with one argument
  - [DatumGetCString](../D/DatumGetCString.md): Extracts C string from PostgreSQL Datum
  - PyObject_CallFunction: Python C API function to call the Decimal constructor
  - PLy_elog: PL/Python error logging function
- Called from (representative examples):
  - [PLy_input_setup_func](PLy_input_setup_func.md): Sets up input conversion functions for various PostgreSQL data types

## Notes and Other Information
- The function uses static storage for decimal_constructor to cache the constructor across multiple calls
- Implements a fallback mechanism: tries cdecimal first (faster C implementation), then falls back to pure Python decimal module
- Preserves full precision by using string-based conversion rather than floating-point intermediate representation
- Error handling includes specific error messages for module import failures and conversion failures
- The function is part of PostgreSQL's strategy to provide high-precision decimal arithmetic in Python stored procedures
- cdecimal was the faster C implementation of decimal arithmetic before it was integrated into Python 3.3's standard library
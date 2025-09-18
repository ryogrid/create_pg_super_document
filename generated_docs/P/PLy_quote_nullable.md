# PLy_quote_nullable

## Location
src/pl/plpython/plpy_plpymodule.c: 340 - 359

## Overview
PLy_quote_nullable is a Python C extension function that provides SQL literal quoting functionality with NULL handling to PL/Python stored procedures, allowing Python code to safely quote string values or handle None/NULL values for use in dynamically constructed SQL statements.

## Definition


## Detailed Description
PLy_quote_nullable extends the functionality of PLy_quote_literal by adding proper handling for NULL values. When a NULL pointer (representing Python None) is passed as an argument, the function returns the string "NULL" suitable for SQL statements. When a non-NULL string is provided, it behaves identically to PLy_quote_literal by calling quote_literal_cstr to properly quote and escape the string.

This function is exposed to Python as `plpy.quote_nullable()` and is particularly useful when constructing dynamic SQL queries where values might be NULL/None, eliminating the need for separate NULL checking in the Python code.

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in static context)
- `args`: Positional arguments tuple containing a single string argument (or None) to be quoted

## Dependencies
- Functions called/Symbols referenced:
  - PyArg_ParseTuple (parses Python arguments with "z" format for nullable string)
  - [PLyUnicode_FromString](PLyUnicode_FromString.md) (converts C string to Python string, called twice)
  - [quote_literal_cstr](../q/quote_literal_cstr.md) (PostgreSQL's core literal quoting function)
  - [pfree](../p/pfree.md) (frees allocated memory)
- Called from (representative examples):
  - Exposed to Python as `plpy.quote_nullable` method in the plpy module

## Notes and Other Information
- This function is registered in the plpy module's method table as "quote_nullable" with METH_VARARGS flag
- Uses "z:quote_nullable" format string in PyArg_ParseTuple, where "z" allows for NULL/None values
- Returns "NULL" (as a string) when the input is NULL/None
- Returns properly quoted string when input is a valid string
- Essential for handling optional/nullable values in dynamic SQL construction
- Part of the PL/Python extension's public API for safe SQL construction with NULL handling
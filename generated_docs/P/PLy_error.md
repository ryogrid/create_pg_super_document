# PLy_error

## Location
src/pl/plpython/plpy_plpymodule.c: 311 - 316

## Overview
PLy_error is a Python C extension function that provides Python code within PL/Python stored procedures a way to raise ERROR-level messages to PostgreSQL's logging and error handling system.

## Definition


## Detailed Description
PLy_error serves as a thin wrapper around the more general PLy_output function, specifically configured to emit ERROR-level messages. This function is exposed to Python code as `plpy.error()` within PL/Python stored procedures and functions. When called, it generates a PostgreSQL ERROR, which will typically abort the current transaction and return the error to the client.

The function accepts both positional and keyword arguments, allowing for flexible error message construction with optional additional error context such as detail, hint, sqlstate, and error location information (schema, table, column names, etc.).

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in static context)
- `args`: Positional arguments tuple containing the error message(s)
- `kw`: Keyword arguments dictionary for additional error context (detail, hint, sqlstate, schema_name, table_name, column_name, datatype_name, constraint_name)

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_output](PLy_output.md) (with ERROR level parameter)
- Called from (representative examples):
  - Exposed to Python as `plpy.error` method in the plpy module

## Notes and Other Information
- This function is registered in the plpy module's method table as "error" with METH_VARARGS | METH_KEYWORDS flags
- The actual error processing and PostgreSQL integration is handled by PLy_output
- When called, this will typically terminate the current transaction with an ERROR
- Part of the PL/Python extension's public API available to stored procedure authors
- The ERROR level corresponds to PostgreSQL's standard error severity that aborts transactions
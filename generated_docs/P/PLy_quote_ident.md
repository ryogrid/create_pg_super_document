# PLy_quote_ident

## Location
[src/pl/plpython/plpy_plpymodule.c:360-376](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L360-L376)

## Overview
PLy_quote_ident is a Python C extension function that provides SQL identifier quoting functionality to PL/Python stored procedures, allowing Python code to safely quote identifiers (table names, column names, etc.) for use in dynamically constructed SQL statements.

## Definition

```c
static PyObject *
PLy_quote_ident(PyObject *self, PyObject *args)
```
## Detailed Description
PLy_quote_ident wraps PostgreSQL's quote_identifier function to provide safe SQL identifier quoting from within Python code. This function takes a string argument representing an SQL identifier (such as a table name, column name, or schema name) and returns a properly quoted version that can be safely used in SQL statements. Unlike literal quoting, identifier quoting uses double quotes and follows SQL rules for identifiers, including case preservation and handling of reserved words.

The function is exposed to Python as `plpy.quote_ident()` and is essential for constructing dynamic SQL queries safely when incorporating variable identifier names, especially when those identifiers might conflict with SQL reserved words or contain special characters.

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in static context)
- `args`: Positional arguments tuple containing a single string argument representing the identifier to be quoted

## Dependencies
- Functions called/Symbols referenced:
  - [quote_identifier](../q/quote_identifier.md) (PostgreSQL's core identifier quoting function)
  - [PLyUnicode_FromString](PLyUnicode_FromString.md) (converts C string to Python string)
  - PyArg_ParseTuple (parses Python arguments)
- Called from (representative examples):
  - Exposed to Python as `plpy.quote_ident` method in the plpy module

## Notes and Other Information
- This function is registered in the plpy module's method table as "quote_ident" with METH_VARARGS flag
- Parses exactly one string argument using format "s:quote_ident"
- Essential for SQL injection prevention when using dynamic identifier names
- Handles SQL reserved words and special characters in identifiers
- Unlike quote_literal, uses double quotes for SQL identifier quoting rules
- Does not require explicit memory deallocation as quote_identifier returns a statically managed string
- Part of the PL/Python extension's public API for safe SQL identifier construction
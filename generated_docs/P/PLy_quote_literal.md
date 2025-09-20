# PLy_quote_literal

## Location
[src/pl/plpython/plpy_plpymodule.c:323-339](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L323-L339)

## Overview
PLy_quote_literal is a Python C extension function that provides SQL literal quoting functionality to PL/Python stored procedures, allowing Python code to safely quote string values for use in dynamically constructed SQL statements.

## Definition

```c
static PyObject *
PLy_quote_literal(PyObject *self, PyObject *args)
```
## Detailed Description
PLy_quote_literal wraps PostgreSQL's quote_literal_cstr function to provide safe SQL literal quoting from within Python code. This function takes a string argument and returns a properly quoted and escaped version that can be safely used as a literal value in SQL statements. It handles SQL injection prevention by properly escaping single quotes and other special characters according to SQL standards.

The function is exposed to Python as `plpy.quote_literal()` and is essential for constructing dynamic SQL queries safely within PL/Python stored procedures, especially when incorporating user-provided or variable data into SQL statements.

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in static context)
- `args`: Positional arguments tuple containing a single string argument to be quoted

## Dependencies
- Functions called/Symbols referenced:
  - [quote_literal_cstr](../q/quote_literal_cstr.md) (PostgreSQL's core literal quoting function)
  - [PLyUnicode_FromString](PLyUnicode_FromString.md) (converts C string back to Python string)
  - PyArg_ParseTuple (parses Python arguments)
  - [pfree](../p/pfree.md) (frees allocated memory)
- Called from (representative examples):
  - Exposed to Python as `plpy.quote_literal` method in the plpy module

## Notes and Other Information
- This function is registered in the plpy module's method table as "quote_literal" with METH_VARARGS flag
- Parses exactly one string argument using format "s:quote_literal"
- Essential for SQL injection prevention in dynamic query construction
- Returns a new Python string object with the quoted result
- Memory management: allocates result via quote_literal_cstr, then frees it after converting to Python string
- Part of the PL/Python extension's public API for safe SQL construction
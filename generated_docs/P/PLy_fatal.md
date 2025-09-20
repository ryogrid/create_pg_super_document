# PLy_fatal

## Location
[src/pl/plpython/plpy_plpymodule.c:317-322](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L317-L322)

## Overview
PLy_fatal is a Python C extension function that provides Python code within PL/Python stored procedures a way to raise FATAL-level messages to PostgreSQL's logging and error handling system.

## Definition

```c
static PyObject *
PLy_fatal(PyObject *self, PyObject *args, PyObject *kw)
```
## Detailed Description
PLy_fatal serves as a thin wrapper around the more general PLy_output function, specifically configured to emit FATAL-level messages. This function is exposed to Python code as `plpy.fatal()` within PL/Python stored procedures and functions. When called, it generates a PostgreSQL FATAL error, which is more severe than ERROR and will terminate the entire database backend session, disconnecting the client.

FATAL errors are reserved for serious conditions that require terminating the database session, such as system-level problems or corruption. The function accepts both positional and keyword arguments, allowing for flexible error message construction with optional additional error context.

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in static context)
- `args`: Positional arguments tuple containing the fatal message(s)
- `kw`: Keyword arguments dictionary for additional error context (detail, hint, sqlstate, schema_name, table_name, column_name, datatype_name, constraint_name)

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_output](PLy_output.md) (with FATAL level parameter)
- Called from (representative examples):
  - Exposed to Python as `plpy.fatal` method in the plpy module

## Notes and Other Information
- This function is registered in the plpy module's method table as "fatal" with METH_VARARGS | METH_KEYWORDS flags
- The actual error processing and PostgreSQL integration is handled by PLy_output
- FATAL errors terminate the entire database backend session and disconnect the client
- Should be used sparingly and only for serious system-level problems
- Part of the PL/Python extension's public API available to stored procedure authors
- More severe than ERROR - while ERROR aborts transactions, FATAL terminates sessions
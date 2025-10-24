# PLy_notice

## Location
[src/pl/plpython/plpy_plpymodule.c:299-304](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L299-L304)

## Overview
PLy_notice is a Python-callable function that provides notice-level logging capabilities for PL/Python stored procedures and functions in PostgreSQL.

## Definition
```c
static PyObject *PLy_notice(PyObject *self, PyObject *args, PyObject *kw)
```

## Detailed Description
PLy_notice is a wrapper function that facilitates notice-level logging from within Python code executing in PostgreSQL's PL/Python environment. It serves as a Python interface to PostgreSQL's internal logging system, specifically targeting NOTICE level messages. This function is part of the plpy module that provides Python stored procedures with access to PostgreSQL's database functionality and logging infrastructure.

The function delegates all its work to PLy_output, passing the NOTICE log level constant along with the provided arguments. NOTICE level messages are specifically designed for helpful messages to users about query operation, and are typically sent to clients to provide informational feedback during query execution.

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in this static context)
- `args`: Python tuple containing the message and positional arguments to be logged
- `kw`: Python dictionary containing keyword arguments for additional logging details (detail, hint, sqlstate, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_output](PLy_output.md) (the core logging function that handles message formatting and PostgreSQL integration)
  - NOTICE (log level constant defined as 18 in src/include/utils/elog.h:43)
- Called from (representative examples):
  - Available to Python code as plpy.notice() when imported in PL/Python functions

## Notes and Other Information
- This is a static function internal to the PL/Python module implementation
- NOTICE level messages are helpful messages to users about query operation
- Part of a family of logging functions (PLy_debug, PLy_info, PLy_notice, PLy_warning, PLy_log) that provide different log levels
- The actual message processing, formatting, and integration with PostgreSQL's ereport system is handled by PLy_output
- NOTICE messages are typically sent to clients and are visible by default to provide user feedback
- NOTICE level is commonly used for informational messages that don't indicate problems but provide useful information about the operation being performed

## Simplified Source

```c
static PyObject *
PLy_notice(PyObject *self, PyObject *args, PyObject *kw)
{
    // Simple wrapper that routes notice-level messages to core logging function
    return PLy_output(NOTICE, self, args, kw);
}
```
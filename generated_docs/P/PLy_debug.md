# PLy_debug

## Location
[src/pl/plpython/plpy_plpymodule.c:281-286](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L281-L286)

## Overview
PLy_debug is a Python-callable function that provides debug-level logging capabilities for PL/Python stored procedures and functions in PostgreSQL.

## Definition

```c
static PyObject *
PLy_debug(PyObject *self, PyObject *args, PyObject *kw)
```
## Detailed Description
PLy_debug is a wrapper function that facilitates debug-level logging from within Python code executing in PostgreSQL's PL/Python environment. It serves as a Python interface to PostgreSQL's internal logging system, specifically targeting DEBUG2 level messages. This function is part of the plpy module that provides Python stored procedures with access to PostgreSQL's database functionality and logging infrastructure.

The function delegates all its work to PLy_output, passing the DEBUG2 log level constant along with the provided arguments. This allows Python code to generate debug messages that integrate with PostgreSQL's standard logging and error reporting mechanisms.

## Parameters / Member Variables
- `*self`: Standard Python method self parameter (unused in this static context)
- `*args`: Python tuple containing the message and positional arguments to be logged
- `*kw`: Python dictionary containing keyword arguments for additional logging details (detail, hint, sqlstate, etc.)
## Dependencies
- Functions called/Symbols referenced:
  - [PLy_output](PLy_output.md) (the core logging function that handles message formatting and PostgreSQL integration)
  - DEBUG2 (log level constant defined as 13 in src/include/utils/elog.h:30)
- Called from (representative examples):
  - Available to Python code as plpy.debug() when imported in PL/Python functions

## Notes and Other Information
- This is a static function internal to the PL/Python module implementation
- Debug messages are typically only visible when PostgreSQL's log_min_messages setting is configured to DEBUG2 or lower
- Part of a family of logging functions (PLy_debug, PLy_info, PLy_notice, PLy_warning, PLy_log) that provide different log levels
- The actual message processing, formatting, and integration with PostgreSQL's ereport system is handled by PLy_output

## Simplified Source

```c
static PyObject *PLy_debug(PyObject *self, PyObject *args, PyObject *kw) {
    // Delegate to PLy_output with DEBUG2 log level
    return PLy_output(DEBUG2, self, args, kw);
}
```
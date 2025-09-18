# PLy_info

## Location
src/pl/plpython/plpy_plpymodule.c: 293 - 298

## Overview
PLy_info is a Python-callable function that provides informational logging capabilities for PL/Python stored procedures and functions in PostgreSQL.

## Definition
```c
static PyObject *PLy_info(PyObject *self, PyObject *args, PyObject *kw)
```

## Detailed Description
PLy_info is a wrapper function that facilitates informational logging from within Python code executing in PostgreSQL's PL/Python environment. It serves as a Python interface to PostgreSQL's internal logging system, specifically targeting INFO level messages. This function is part of the plpy module that provides Python stored procedures with access to PostgreSQL's database functionality and logging infrastructure.

The function delegates all its work to PLy_output, passing the INFO log level constant along with the provided arguments. INFO level messages are specifically intended for messages requested by users or important informational content that should be visible to both the server log and potentially to clients.

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in this static context)
- `args`: Python tuple containing the message and positional arguments to be logged
- `kw`: Python dictionary containing keyword arguments for additional logging details (detail, hint, sqlstate, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_output](PLy_output.md) (the core logging function that handles message formatting and PostgreSQL integration)
  - INFO (log level constant defined as 17 in src/include/utils/elog.h:39)
- Called from (representative examples):
  - Available to Python code as plpy.info() when imported in PL/Python functions

## Notes and Other Information
- This is a static function internal to the PL/Python module implementation
- INFO level messages are specifically for user-requested messages or important informational content
- Part of a family of logging functions (PLy_debug, PLy_info, PLy_notice, PLy_warning, PLy_log) that provide different log levels
- The actual message processing, formatting, and integration with PostgreSQL's ereport system is handled by PLy_output
- INFO level messages are visible when PostgreSQL's log_min_messages setting is configured to INFO or lower
- Unlike LOG level messages, INFO messages may be sent to both server log and clients depending on configuration
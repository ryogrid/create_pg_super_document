# PLy_log

## Location
src/pl/plpython/plpy_plpymodule.c: 287 - 292

## Overview
PLy_log is a Python-callable function that provides general log-level logging capabilities for PL/Python stored procedures and functions in PostgreSQL.

## Definition
```c
static PyObject *PLy_log(PyObject *self, PyObject *args, PyObject *kw)
```

## Detailed Description
PLy_log is a wrapper function that facilitates general-purpose logging from within Python code executing in PostgreSQL's PL/Python environment. It serves as a Python interface to PostgreSQL's internal logging system, specifically targeting LOG level messages. This function is part of the plpy module that provides Python stored procedures with access to PostgreSQL's database functionality and logging infrastructure.

The function delegates all its work to PLy_output, passing the LOG level constant along with the provided arguments. LOG level messages are typically used for server operational messages and are sent only to the server log, not to the client, making them suitable for internal logging and monitoring purposes.

## Parameters / Member Variables
- `self`: Standard Python method self parameter (unused in this static context)
- `args`: Python tuple containing the message and positional arguments to be logged
- `kw`: Python dictionary containing keyword arguments for additional logging details (detail, hint, sqlstate, etc.)

## Dependencies
- Functions called/Symbols referenced:
  - PLy_output (the core logging function that handles message formatting and PostgreSQL integration)
  - LOG (log level constant defined as 15 in src/include/utils/elog.h:32)
- Called from (representative examples):
  - Available to Python code as plpy.log() when imported in PL/Python functions

## Notes and Other Information
- This is a static function internal to the PL/Python module implementation
- LOG level messages are server operational messages sent only to the server log, not to clients
- Part of a family of logging functions (PLy_debug, PLy_info, PLy_notice, PLy_warning, PLy_log) that provide different log levels
- The actual message processing, formatting, and integration with PostgreSQL's ereport system is handled by PLy_output
- LOG level messages are visible when PostgreSQL's log_min_messages setting is configured to LOG or lower
# PLy_traceback

## Location
src/pl/plpython/plpy_elog.c: 173 - 356

## Overview
PLy_traceback extracts and formats Python exception information and traceback data into PostgreSQL-compatible error message strings, providing detailed debugging information for PL/Python errors.

## Definition


## Detailed Description
PLy_traceback is a static function that processes Python exception objects to create formatted error messages and traceback strings for PostgreSQL error reporting. The function operates in two phases:

1. **Exception Message Formatting**: Extracts the exception type and module information, formats them similar to Python's traceback.format_exception_only(), and creates a readable error message string.

2. **Traceback Processing**: Walks through the Python traceback object chain, extracting frame information (filename, line number, function name) and formatting it into a traceback string that mimics Python's standard traceback output.

The function handles special cases like built-in exceptions, PL/Python function contexts, and source code line extraction for better debugging information. It properly manages Python object reference counting and uses PostgreSQL's exception handling for cleanup.

## Parameters / Member Variables
- : Python exception type object
- : Python exception value/instance object  
- : Python traceback object
- : Output pointer for formatted exception message string (palloc'd)
- : Output pointer for formatted traceback string (palloc'd)
- : Output pointer for traceback depth count

## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_AsString](PLyUnicode_AsString.md): Converts Python strings to C strings
  - [PLy_current_execution_context](PLy_current_execution_context.md): Gets current PL/Python execution context
  - [PLy_procedure_name](PLy_procedure_name.md): Gets procedure name from execution context
  - [get_source_line](../g/get_source_line.md): Extracts specific source code line
  - PG_TRY/PG_FINALLY/PG_END_TRY: PostgreSQL exception handling macros
- Called from (representative examples):
  - [PLy_elog_impl](PLy_elog_impl.md): Main error logging function

## Notes and Other Information
- Function mimics Python's traceback module behavior for consistency
- Skips the first frame (module level) and handles the second frame specially  
- Extracts actual source code lines for PL/Python functions when available
- Properly handles Python object cleanup with Py_XDECREF in PG_FINALLY blocks
- Uses StringInfo for efficient string building
- Returns NULL values when no exception is present
- Handles both built-in and custom exception modules appropriately
- Function is located in src/pl/plpython/plpy_elog.c:173-356
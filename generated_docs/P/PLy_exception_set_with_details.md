# PLy_exception_set_with_details

## Location
src/pl/plpython/plpy_elog.c: 509 - 566

## Overview
Creates a comprehensive Python exception object populated with detailed error information from PostgreSQL's ErrorData structure, providing rich error context for PL/Python exception handling.

## Definition
```c
void PLy_exception_set_with_details(PyObject *excclass, ErrorData *edata)
```

## Detailed Description
This function creates a richly detailed Python exception by extracting information from PostgreSQL's ErrorData structure and populating various exception attributes. It creates a new instance of the specified exception class with the error message, then systematically sets multiple string attributes including SQL state, detail message, hint, query text, and schema/table/column/datatype/constraint names. This provides Python code with comprehensive context about database errors, enabling more sophisticated error handling and debugging. The function uses proper Python reference counting and includes error handling for cases where Python object creation fails.

## Parameters / Member Variables
- `excclass`: Python exception class to instantiate
- `edata`: PostgreSQL ErrorData structure containing detailed error information

## Dependencies
- Functions called/Symbols referenced:
  - Py_BuildValue (Python C API function)
  - PyObject_CallObject (Python C API function)
  - [set_string_attr](../s/set_string_attr.md) (helper function for setting object attributes)
  - [unpack_sql_state](../u/unpack_sql_state.md) (function to format SQL state codes)
  - PyErr_SetObject (Python C API function)
  - Py_DECREF/Py_XDECREF (Python reference counting functions)
  - elog (PostgreSQL logging function)
  - ErrorData (PostgreSQL error data structure)
- Called from (representative examples):
  - [PLy_output](PLy_output.md)

## Notes and Other Information
- Provides comprehensive error details including SQL state, hints, query context, and object names
- Uses proper Python reference counting to avoid memory leaks
- Includes robust error handling with cleanup on failure
- Falls back to PostgreSQL's elog(ERROR) if Python exception creation fails
- Essential for bridging PostgreSQL's rich error system with Python's exception model
- Sets multiple exception attributes: sqlstate, detail, hint, query, schema_name, table_name, column_name, datatype_name, constraint_name
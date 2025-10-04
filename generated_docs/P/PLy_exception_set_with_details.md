# PLy_exception_set_with_details

## Location
[src/pl/plpython/plpy_elog.c:509-566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_elog.c#L509-L566)

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
  - [ErrorData](../E/ErrorData.md) (PostgreSQL error data structure)
- Called from (representative examples):
  - [PLy_output](PLy_output.md)

## Notes and Other Information
- Provides comprehensive error details including SQL state, hints, query context, and object names
- Uses proper Python reference counting to avoid memory leaks
- Includes robust error handling with cleanup on failure
- Falls back to PostgreSQL's elog(ERROR) if Python exception creation fails
- Essential for bridging PostgreSQL's rich error system with Python's exception model
- Sets multiple exception attributes: sqlstate, detail, hint, query, schema_name, table_name, column_name, datatype_name, constraint_name

## Simplified Source

```c
void PLy_exception_set_with_details(PyObject *excclass, ErrorData *edata) {
    PyObject *args = NULL;
    PyObject *error = NULL;

    // Create exception instance with error message
    args = Py_BuildValue("(s)", edata->message);
    if (!args)
        goto failure;

    error = PyObject_CallObject(excclass, args);
    if (!error)
        goto failure;

    // Set all error detail attributes on the exception object
    if (!set_string_attr(error, "sqlstate", unpack_sql_state(edata->sqlerrcode)))
        goto failure;
    if (!set_string_attr(error, "detail", edata->detail))
        goto failure;
    if (!set_string_attr(error, "hint", edata->hint))
        goto failure;
    if (!set_string_attr(error, "query", edata->internalquery))
        goto failure;
    if (!set_string_attr(error, "schema_name", edata->schema_name))
        goto failure;
    if (!set_string_attr(error, "table_name", edata->table_name))
        goto failure;
    if (!set_string_attr(error, "column_name", edata->column_name))
        goto failure;
    if (!set_string_attr(error, "datatype_name", edata->datatype_name))
        goto failure;
    if (!set_string_attr(error, "constraint_name", edata->constraint_name))
        goto failure;

    // Set the Python exception
    PyErr_SetObject(excclass, error);

    // Cleanup
    Py_DECREF(args);
    Py_DECREF(error);
    return;

failure:
    Py_XDECREF(args);
    Py_XDECREF(error);
    elog(ERROR, "could not convert error to Python exception");
}
```
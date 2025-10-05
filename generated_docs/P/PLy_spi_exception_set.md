# PLy_spi_exception_set

## Location
[src/pl/plpython/plpy_spi.c:621-658](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_spi.c#L621-L658)

## Overview
A static function within PostgreSQL's PL/Python extension that creates and raises Python exceptions from PostgreSQL SPI (Server Programming Interface) error data, providing detailed error information to Python code.

## Definition

```c
static void
PLy_spi_exception_set(PyObject *excclass, ErrorData *edata)
```
## Detailed Description
PLy_spi_exception_set is responsible for converting PostgreSQL internal error data (ErrorData) into Python exceptions that can be handled by PL/Python code. This function serves as a bridge between PostgreSQL's internal error handling system and Python's exception mechanism.

The function creates a Python exception object of the specified class, populates it with the error message, and attaches additional structured error information as a 'spidata' attribute. This allows Python code to access detailed error context including SQL error codes, hints, internal query information, and schema details.

The function implements careful memory management for Python objects, using proper reference counting and cleanup on failure paths to prevent memory leaks.

## Parameters / Member Variables
- `*excclass`: PyObject pointer to the Python exception class to instantiate (e.g., SPIError)
- `*edata`: Pointer to PostgreSQL's ErrorData structure containing detailed error information
## Dependencies
- Functions called/Symbols referenced:
  - [ErrorData](../E/ErrorData.md) (PostgreSQL error data structure)
  - Py_BuildValue (Python C API function for creating Python objects)
  - PyObject_CallObject (Python C API function for calling Python objects)
  - PyObject_SetAttrString (Python C API function for setting object attributes)
  - PyErr_SetObject (Python C API function for setting Python exceptions)
  - Py_DECREF/Py_XDECREF (Python reference counting functions)
  - elog (PostgreSQL logging function)

- Called from (representative examples):
  - [PLy_commit](PLy_commit.md) (transaction commit handling)
  - [PLy_rollback](PLy_rollback.md) (transaction rollback handling) 
  - [PLy_spi_subtransaction_abort](PLy_spi_subtransaction_abort.md) (subtransaction abort handling)

## Notes and Other Information
- This is a static function, meaning it's only accessible within the same source file (plpy_spi.c)
- The function creates a structured 'spidata' tuple containing 10 elements from ErrorData: sqlerrcode, detail, hint, internalquery, internalpos, schema_name, table_name, column_name, datatype_name, and constraint_name
- Error handling follows the Python C API pattern with proper cleanup using goto failure pattern
- If the function fails to create the Python exception objects, it falls back to using PostgreSQL's elog(ERROR) to report the conversion failure
- The function is part of the PL/Python extension's SPI interface, which allows Python stored procedures to interact with the database

## Simplified Source

```c
static void PLy_spi_exception_set(PyObject *excclass, ErrorData *edata) {
    PyObject *args = NULL;
    PyObject *spierror = NULL;
    PyObject *spidata = NULL;

    // Create exception with error message
    args = Py_BuildValue("(s)", edata->message);
    if (!args) goto failure;

    spierror = PyObject_CallObject(excclass, args);
    if (!spierror) goto failure;

    // Build detailed error data tuple
    spidata = Py_BuildValue("(izzzizzzzz)",
                           edata->sqlerrcode, edata->detail, edata->hint,
                           edata->internalquery, edata->internalpos,
                           edata->schema_name, edata->table_name, edata->column_name,
                           edata->datatype_name, edata->constraint_name);
    if (!spidata) goto failure;

    // Attach detailed data to exception
    if (PyObject_SetAttrString(spierror, "spidata", spidata) == -1)
        goto failure;

    // Raise the Python exception
    PyErr_SetObject(excclass, spierror);

    // Cleanup and return
    Py_DECREF(args);
    Py_DECREF(spierror);
    Py_DECREF(spidata);
    return;

failure:
    // Cleanup on error
    Py_XDECREF(args);
    Py_XDECREF(spierror);
    Py_XDECREF(spidata);
    elog(ERROR, "could not convert SPI error to Python exception");
}
```
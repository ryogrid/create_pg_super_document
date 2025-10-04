# PLy_output

## Location
[src/pl/plpython/plpy_plpymodule.c:398-561](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L398-L561)

## Overview
A comprehensive logging and error reporting function for PL/Python that handles message formatting and PostgreSQL error reporting with various severity levels and detailed error context information.

## Definition
static PyObject *PLy_output(volatile int level, PyObject *self, PyObject *args, PyObject *kw)

## Detailed Description
This function serves as the core implementation for PL/Pythons logging and error reporting system. It processes Python function arguments and keyword arguments to construct detailed PostgreSQL error reports with various severity levels (DEBUG, LOG, INFO, NOTICE, WARNING, ERROR, FATAL). The function handles both positional and keyword arguments, allowing for rich error context including SQL state codes, detailed messages, hints, and schema/table/column information.

The function is designed to integrate seamlessly with PostgreSQLs error reporting infrastructure while providing a Pythonic interface for PL/Python procedures. It includes comprehensive validation for SQL state codes and ensures proper memory management across Python and PostgreSQL memory contexts.

## Parameters / Member Variables
- level: An integer representing the severity level of the message (DEBUG, LOG, INFO, NOTICE, WARNING, ERROR, FATAL)
- self: Standard Python method self parameter (unused in this static context)
- args: PyObject tuple containing positional arguments for the message
- kw: PyObject dictionary containing keyword arguments for detailed error context

## Dependencies
- Functions called/Symbols referenced:
  - PyTuple_Size, PyArg_UnpackTuple, PyObject_Str (Python C API)
  - PLy_elog (PL/Python error handling)
  - [PLyUnicode_AsString](PLyUnicode_AsString.md) (PL/Python string conversion)
  - dgettext, TEXTDOMAIN (internationalization)
  - [object_to_string](../o/object_to_string.md) (string conversion utility)
  - [PLy_exception_set](PLy_exception_set.md) (PL/Python exception handling)
  - MAKE_SQLSTATE (PostgreSQL SQL state handling)
  - [pg_verifymbstr](../p/pg_verifymbstr.md) (PostgreSQL string validation)
  - ereport, errcode, errmsg_internal, errdetail_internal, errhint (PostgreSQL error reporting)
  - [err_generic_string](../e/err_generic_string.md) with PG_DIAG_* constants (PostgreSQL diagnostic fields)
  - PG_TRY/PG_CATCH/PG_END_TRY (PostgreSQL exception handling)
  - [CopyErrorData](../C/CopyErrorData.md), FlushErrorState, FreeErrorData (PostgreSQL error data management)
  - [PLy_exception_set_with_details](PLy_exception_set_with_details.md) (PL/Python detailed exception handling)
- Called from (representative examples):
  - [PLy_debug](PLy_debug.md), PLy_log, PLy_info, PLy_notice, PLy_warning, PLy_error, PLy_fatal
  - [PLy_generate_spi_exceptions](PLy_generate_spi_exceptions.md)

## Notes and Other Information
- Supports both single argument and multiple argument message formatting
- Validates keyword arguments and rejects unknown parameters
- Handles SQL state codes with strict validation (5 character alphanumeric codes)
- Provides comprehensive error context fields including schema_name, table_name, column_name, datatype_name, constraint_name
- Uses PostgreSQL memory context switching for proper resource management
- Validates all string inputs for proper multi-byte encoding
- Returns Py_None on successful completion or NULL on error
- Critical for all logging and error reporting functionality in PL/Python procedures
- Integrates with PostgreSQL error reporting system to provide consistent error handling across the database system

## Simplified Source

```c
static PyObject *PLy_output(volatile int level, PyObject *self, PyObject *args, PyObject *kw) {
    int sqlstate = 0;
    char *message = NULL;
    char *detail = NULL;
    char *hint = NULL;
    char *schema_name = NULL;
    char *table_name = NULL;
    char *column_name = NULL;
    char *datatype_name = NULL;
    char *constraint_name = NULL;

    // Handle message arguments
    if (PyTuple_Size(args) == 1) {
        PyObject *o;
        if (!PyArg_UnpackTuple(args, "plpy.elog", 1, 1, &o))
            PLy_elog(ERROR, "could not unpack arguments in plpy.elog");
        PyObject *so = PyObject_Str(o);
        message = PLyUnicode_AsString(so);
        Py_XDECREF(so);
    } else {
        PyObject *so = PyObject_Str(args);
        message = PLyUnicode_AsString(so);
        Py_XDECREF(so);
    }

    if (message == NULL) {
        level = ERROR;
        message = "could not parse error message in plpy.elog";
    }
    message = pstrdup(message);

    // Process keyword arguments for error context
    if (kw != NULL) {
        PyObject *key, *value;
        Py_ssize_t pos = 0;

        while (PyDict_Next(kw, &pos, &key, &value)) {
            char *keyword = PLyUnicode_AsString(key);

            if (strcmp(keyword, "message") == 0)
                message = object_to_string(value);
            else if (strcmp(keyword, "detail") == 0)
                detail = object_to_string(value);
            else if (strcmp(keyword, "hint") == 0)
                hint = object_to_string(value);
            else if (strcmp(keyword, "sqlstate") == 0) {
                char *sqlstatestr = object_to_string(value);
                if (strlen(sqlstatestr) == 5 &&
                    strspn(sqlstatestr, "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ") == 5) {
                    sqlstate = MAKE_SQLSTATE(sqlstatestr[0], sqlstatestr[1],
                                           sqlstatestr[2], sqlstatestr[3], sqlstatestr[4]);
                }
            }
            else if (strcmp(keyword, "schema_name") == 0)
                schema_name = object_to_string(value);
            else if (strcmp(keyword, "table_name") == 0)
                table_name = object_to_string(value);
            else if (strcmp(keyword, "column_name") == 0)
                column_name = object_to_string(value);
            else if (strcmp(keyword, "datatype_name") == 0)
                datatype_name = object_to_string(value);
            else if (strcmp(keyword, "constraint_name") == 0)
                constraint_name = object_to_string(value);
            else {
                PLy_exception_set(PyExc_TypeError, "'%s' is an invalid keyword argument", keyword);
                return NULL;
            }
        }
    }

    // Validate strings and report error
    PG_TRY();
    {
        // Validate all string inputs
        if (message) pg_verifymbstr(message, strlen(message), false);
        if (detail) pg_verifymbstr(detail, strlen(detail), false);
        if (hint) pg_verifymbstr(hint, strlen(hint), false);
        // ... additional validations for other fields

        // Report the error with all context
        ereport(level,
            ((sqlstate != 0) ? errcode(sqlstate) : 0,
             (message != NULL) ? errmsg_internal("%s", message) : 0,
             (detail != NULL) ? errdetail_internal("%s", detail) : 0,
             (hint != NULL) ? errhint("%s", hint) : 0,
             (column_name != NULL) ? err_generic_string(PG_DIAG_COLUMN_NAME, column_name) : 0,
             (constraint_name != NULL) ? err_generic_string(PG_DIAG_CONSTRAINT_NAME, constraint_name) : 0,
             (datatype_name != NULL) ? err_generic_string(PG_DIAG_DATATYPE_NAME, datatype_name) : 0,
             (table_name != NULL) ? err_generic_string(PG_DIAG_TABLE_NAME, table_name) : 0,
             (schema_name != NULL) ? err_generic_string(PG_DIAG_SCHEMA_NAME, schema_name) : 0));
    }
    PG_CATCH();
    {
        // Handle any errors during reporting
        ErrorData *edata = CopyErrorData();
        FlushErrorState();
        PLy_exception_set_with_details(PLy_exc_error, edata);
        FreeErrorData(edata);
        return NULL;
    }
    PG_END_TRY();

    Py_RETURN_NONE;
}
```
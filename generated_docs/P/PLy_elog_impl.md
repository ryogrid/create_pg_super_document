# PLy_elog_impl

## Location
[src/pl/plpython/plpy_elog.c:44-172](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_elog.c#L44-L172)

## Overview
PLy_elog_impl is the core implementation function for propagating Python errors into PostgreSQL error reporting system, converting Python exceptions into PostgreSQL errors or notices with proper error fields and traceback context.

## Definition

```c
void
PLy_elog_impl(int elevel, const char *fmt,...)
```
## Detailed Description
PLy_elog_impl serves as the bridge between Python's exception system and PostgreSQL's error reporting mechanism. The function takes Python exceptions previously captured by PLy_exception_set() and converts them into PostgreSQL errors with appropriate error levels, messages, and context information.

The function operates in two modes:
1. When  is provided: The formatted message becomes the primary error message, and any Python exception message becomes the error detail
2. When  is NULL: The Python exception message becomes the primary error message

The function handles special PostgreSQL-specific exception types (PLy_exc_spi_error, PLy_exc_error, PLy_exc_fatal) by extracting structured error information like SQL error codes, hints, and database object names. It also processes Python tracebacks and includes them as error context.

## Parameters / Member Variables
- : PostgreSQL error level (ERROR, WARNING, NOTICE, etc.)
- : Optional format string for the primary error message (can be NULL)
- : Variable arguments for the format string

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_get_spi_error_data](PLy_get_spi_error_data.md): Extracts error data from SPI exceptions
  - [PLy_get_error_data](PLy_get_error_data.md): Extracts error data from general PostgreSQL exceptions  
  - [PLy_traceback](PLy_traceback.md): Processes Python traceback for context information
  - [appendStringInfoVA](../a/appendStringInfoVA.md): Formats variable argument strings
  - ereport: PostgreSQL's main error reporting function
  - PG_TRY/PG_FINALLY/PG_END_TRY: PostgreSQL exception handling macros
- Called from (representative examples):
  - PLy_elog: Macro wrapper for this function

## Notes and Other Information
- Uses PostgreSQL's PG_TRY exception handling to ensure proper cleanup of Python objects
- Properly handles Python reference counting with Py_XDECREF for exception objects
- Supports internationalization through dgettext for error messages
- Maintains errno value across the function call
- Processes complete traceback chains by walking tb_next attributes
- Memory management includes cleanup of StringInfo buffers and temporary strings
- Function is located in src/pl/plpython/plpy_elog.c:44-172

## Simplified Source

```c
void PLy_elog_impl(int elevel, const char *fmt, ...) {
    char *exception_msg = NULL;
    char *traceback_msg = NULL;
    StringInfoData formatted_msg;
    PyObject *exc, *val, *tb;

    // Initialize message buffer if format string provided
    if (fmt)
        initStringInfo(&formatted_msg);

    // Get current Python exception
    PyErr_Fetch(&exc, &val, &tb);

    PG_TRY(); {
        // Initialize error fields
        const char *primary_msg = NULL;
        int sql_errcode = 0;
        char *detail = NULL, *hint = NULL, *query = NULL;
        int position = 0;
        char *schema_name = NULL, *table_name = NULL, *column_name = NULL;
        char *datatype_name = NULL, *constraint_name = NULL;

        // Extract error data from specific exception types
        if (exc != NULL) {
            PyErr_NormalizeException(&exc, &val, &tb);

            if (PyErr_GivenExceptionMatches(val, PLy_exc_spi_error))
                PLy_get_spi_error_data(val, &sql_errcode, &detail, &hint, &query, &position,
                                     &schema_name, &table_name, &column_name,
                                     &datatype_name, &constraint_name);
            else if (PyErr_GivenExceptionMatches(val, PLy_exc_error))
                PLy_get_error_data(val, &sql_errcode, &detail, &hint,
                                 &schema_name, &table_name, &column_name,
                                 &datatype_name, &constraint_name);
            else if (PyErr_GivenExceptionMatches(val, PLy_exc_fatal))
                elevel = FATAL;
        }

        // Get traceback information
        PLy_traceback(exc, val, tb, &exception_msg, &traceback_msg, &tb_depth);

        // Format primary message
        if (fmt) {
            // Format the provided message string
            va_list ap;
            va_start(ap, fmt);
            appendStringInfoVA(&formatted_msg, dgettext(TEXTDOMAIN, fmt), ap);
            va_end(ap);
            primary_msg = formatted_msg.data;

            // Python exception becomes detail if present
            if (exception_msg)
                detail = exception_msg;
        } else {
            // Python exception becomes primary message
            if (exception_msg)
                primary_msg = exception_msg;
        }

        // Report the error with all collected information
        ereport(elevel,
                (errcode(sql_errcode ? sql_errcode : ERRCODE_EXTERNAL_ROUTINE_EXCEPTION),
                 errmsg_internal("%s", primary_msg ? primary_msg : "no exception data"),
                 detail ? errdetail_internal("%s", detail) : 0,
                 (tb_depth > 0 && traceback_msg) ? errcontext("%s", traceback_msg) : 0,
                 hint ? errhint("%s", hint) : 0,
                 query ? internalerrquery(query) : 0,
                 position ? internalerrposition(position) : 0,
                 schema_name ? err_generic_string(PG_DIAG_SCHEMA_NAME, schema_name) : 0,
                 table_name ? err_generic_string(PG_DIAG_TABLE_NAME, table_name) : 0,
                 column_name ? err_generic_string(PG_DIAG_COLUMN_NAME, column_name) : 0,
                 datatype_name ? err_generic_string(PG_DIAG_DATATYPE_NAME, datatype_name) : 0,
                 constraint_name ? err_generic_string(PG_DIAG_CONSTRAINT_NAME, constraint_name) : 0));
    }
    PG_FINALLY(); {
        // Cleanup Python objects and allocated memory
        Py_XDECREF(exc);
        Py_XDECREF(val);

        // Release traceback chain
        while (tb != NULL && tb != Py_None) {
            PyObject *tb_prev = tb;
            tb = PyObject_GetAttrString(tb, "tb_next");
            Py_DECREF(tb_prev);
        }

        // Free string buffers
        if (fmt) pfree(formatted_msg.data);
        if (exception_msg) pfree(exception_msg);
        if (traceback_msg) pfree(traceback_msg);
    }
    PG_END_TRY();
}
```
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
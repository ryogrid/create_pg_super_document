# PLy_get_spi_error_data

## Location
src/pl/plpython/plpy_elog.c: 381 - 416

## Overview
PLy_get_spi_error_data extracts comprehensive error information from Python SPIError exception objects, including SQL error codes, messages, database object identifiers, and query position data.

## Definition


## Detailed Description
PLy_get_spi_error_data is a specialized function for extracting detailed error information from SPI (Server Programming Interface) errors in PL/Python. The function attempts to parse the 'spidata' attribute from SPIError exceptions, which contains structured error information as a Python tuple.

The function uses PyArg_ParseTuple with format string "izzzizzzzz" to extract:
- An integer SQL error code
- String pointers for detail, hint, query text
- An integer query position
- String pointers for various database object names

If the spidata attribute is not present (e.g., when someone manually raises an SPIError from Python code), the function falls back to extracting just the SQL error code using PLy_get_sqlerrcode.

## Parameters / Member Variables
- : Python SPIError exception object
- : Output pointer for SQL error code
- : Output pointer for error detail message
- : Output pointer for error hint message  
- : Output pointer for the query text that caused the error
- : Output pointer for character position in query where error occurred
- : Output pointer for schema name involved in error
- : Output pointer for table name involved in error
- : Output pointer for column name involved in error
- : Output pointer for data type name involved in error
- : Output pointer for constraint name involved in error

## Dependencies
- Functions called/Symbols referenced:
  - PLy_get_sqlerrcode: Fallback function to extract SQL error code
  - PyArg_ParseTuple: Python C API function for tuple parsing
  - Py_XDECREF: Python reference counting cleanup
- Called from (representative examples):
  - PLy_elog_impl: Main error logging function for SPI errors

## Notes and Other Information
- Part of the PL/Python SPI error handling subsystem
- Handles both structured SPI errors and manually raised SPIErrors
- Uses format string "izzzizzzzz" for parsing tuple: integer, strings, integer, strings
- Proper Python object cleanup with Py_XDECREF
- Provides comprehensive database context for error reporting
- Function is located in src/pl/plpython/plpy_elog.c:381-416
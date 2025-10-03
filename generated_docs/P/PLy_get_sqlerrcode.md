# PLy_get_sqlerrcode

## Location
[src/pl/plpython/plpy_elog.c:357-380](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_elog.c#L357-L380)

## Overview
PLy_get_sqlerrcode extracts and validates SQL state error codes from Python SPIError exception objects, converting 5-character SQL state strings into PostgreSQL's internal error code format.

## Definition

```c
static void
PLy_get_sqlerrcode(PyObject *exc, int *sqlerrcode)
```
## Detailed Description
PLy_get_sqlerrcode is a utility function that extracts the 'sqlstate' attribute from Python exception objects (typically SPIError exceptions) and converts it into PostgreSQL's internal SQLSTATE representation. The function validates that the sqlstate is exactly 5 characters long and contains only valid alphanumeric characters (0-9, A-Z) as required by the SQL standard.

The function uses PostgreSQL's MAKE_SQLSTATE macro to convert the 5-character string into an internal integer representation that can be used with PostgreSQL's error reporting system. If the sqlstate attribute is missing or invalid, the function silently returns without modifying the output parameter.

## Parameters / Member Variables
- `*exc`: Python exception object containing sqlstate attribute
- `*sqlerrcode`: Output pointer to store the converted SQL error code
## Dependencies
- Functions called/Symbols referenced:
  - [PLyUnicode_AsString](PLyUnicode_AsString.md): Converts Python Unicode string to C string
  - MAKE_SQLSTATE: PostgreSQL macro to create SQLSTATE from 5 characters
- Called from (representative examples):
  - [PLy_get_spi_error_data](PLy_get_spi_error_data.md): Processes SPI-specific error information
  - [PLy_get_error_data](PLy_get_error_data.md): Processes general PostgreSQL error information

## Notes and Other Information
- Validates sqlstate format: exactly 5 characters, alphanumeric only
- Uses strspn for efficient character set validation
- Properly handles Python object reference counting with Py_DECREF
- Silent failure mode - doesn't raise errors for invalid/missing sqlstate
- Part of the PL/Python error handling subsystem
- Function is located in src/pl/plpython/plpy_elog.c:357-380
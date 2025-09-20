# PLy_get_error_data

## Location
[src/pl/plpython/plpy_elog.c:417-434](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_elog.c#L417-L434)

## Overview
PLy_get_error_data extracts error information from Python Error exception objects, retrieving SQL error codes and database object context information for PostgreSQL error reporting.

## Definition

```c
static void
PLy_get_error_data(PyObject *exc, int *sqlerrcode, char **detail, char **hint,
				   char **schema_name, char **table_name, char **column_name,
				   char **datatype_name, char **constraint_name)
```
## Detailed Description
PLy_get_error_data is designed to extract error information from general PostgreSQL Error exceptions (as opposed to the more specific SPIError exceptions). Unlike PLy_get_spi_error_data, this function does not handle query position or query text attributes since these are not applicable to general Error exceptions.

The function systematically extracts string attributes from the Python exception object using the get_string_attr helper function, retrieving various pieces of context information that can provide detailed database object identification in error reports. It first extracts the SQL error code using PLy_get_sqlerrcode, then proceeds to extract all available string attributes.

## Parameters / Member Variables
- : Python Error exception object
- : Output pointer for SQL error code
- : Output pointer for error detail message
- : Output pointer for error hint message
- : Output pointer for schema name involved in error
- : Output pointer for table name involved in error
- : Output pointer for column name involved in error
- : Output pointer for data type name involved in error
- : Output pointer for constraint name involved in error

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_get_sqlerrcode](PLy_get_sqlerrcode.md): Extracts SQL error code from exception
  - [get_string_attr](../g/get_string_attr.md): Helper function to extract string attributes from Python objects
- Called from (representative examples):
  - [PLy_elog_impl](PLy_elog_impl.md): Main error logging function for general PostgreSQL errors

## Notes and Other Information
- Designed for general Error exceptions, not SPI-specific errors
- Does not handle query position or query text (unlike PLy_get_spi_error_data)
- Uses systematic attribute extraction pattern with get_string_attr helper
- Part of the PL/Python error handling subsystem
- Provides database object context for comprehensive error reporting
- Simpler than PLy_get_spi_error_data due to fewer error attributes
- Function is located in src/pl/plpython/plpy_elog.c:417-434
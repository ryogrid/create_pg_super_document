# PLy_generate_spi_exceptions

## Location
src/pl/plpython/plpy_plpymodule.c: 240 - 280

## Overview
PLy_generate_spi_exceptions creates Python exception classes for all PostgreSQL SPI error codes, establishing a complete mapping between database error conditions and Python exceptions.

## Definition
```c
static void PLy_generate_spi_exceptions(PyObject *mod, PyObject *base)
```

## Detailed Description
This function iterates through a predefined exception_map array to create Python exception classes for every PostgreSQL SPI (Server Programming Interface) error condition. Each exception is created as a subclass of SPIError, includes the corresponding SQL state code as an attribute, and is registered in both the spiexceptions module and the global SPI exceptions hash table. This comprehensive mapping enables PL/Python code to catch specific database errors using Python's exception handling mechanisms, providing fine-grained error handling capabilities.

## Parameters / Member Variables
- `mod`: PyObject pointer to the spiexceptions module where exceptions will be added
- `base`: PyObject pointer to the base SPIError exception class

## Dependencies
- Functions called/Symbols referenced:
  - PyDict_New (Python C API)
  - PLy_elog
  - [PLyUnicode_FromString](PLyUnicode_FromString.md)
  - [unpack_sql_state](../u/unpack_sql_state.md)
  - PyDict_SetItemString (Python C API)
  - Py_DECREF (Python C API)
  - [PLy_create_exception](PLy_create_exception.md)
  - [hash_search](../h/hash_search.md)
  - Assert (PostgreSQL assertion macro)
  - HASH_ENTER (hash operation flag)
  - [PLyExceptionEntry](PLyExceptionEntry.md) (exception entry structure)
  - exception_map (global exception mapping array)
- Called from (representative examples):
  - [PLy_add_exceptions](PLy_add_exceptions.md)

## Notes and Other Information
- Processes a complete mapping of PostgreSQL error codes to Python exceptions
- Each exception includes the SQL state as a 'sqlstate' attribute accessible from Python
- Uses PostgreSQL's hash table system for efficient exception lookup by SQL state
- Creates a comprehensive exception hierarchy allowing fine-grained error handling in PL/Python
- The exception_map array defines the complete set of PostgreSQL SPI exceptions to be generated
- Essential for enabling PL/Python functions to catch and handle specific database error conditions
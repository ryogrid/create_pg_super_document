# PLy_add_exceptions

## Location
[src/pl/plpython/plpy_plpymodule.c:175-210](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L175-L210)

## Overview
PLy_add_exceptions sets up the exception hierarchy and SPI exception handling system for the PL/Python language extension, creating Python exception classes that correspond to PostgreSQL error conditions.

## Definition
```c
static void PLy_add_exceptions(PyObject *plpy)
```

## Detailed Description
This function establishes the complete exception handling framework for PL/Python by creating a dedicated spiexceptions module and defining core exception classes. It creates fundamental exception types (Error, Fatal, SPIError), sets up a hash table to efficiently map PostgreSQL error codes to Python exceptions, and generates all SPI-specific exceptions. The function ensures that PL/Python code can catch and handle database errors using standard Python exception handling mechanisms.

## Parameters / Member Variables
- `plpy`: PyObject pointer to the plpy module where exceptions will be added

## Dependencies
- Functions called/Symbols referenced:
  - PyModule_Create (Python C API)
  - PLy_elog
  - Py_INCREF (Python C API)
  - PyModule_AddObject (Python C API)
  - [PLy_create_exception](PLy_create_exception.md)
  - [hash_create](../h/hash_create.md)
  - [PLy_generate_spi_exceptions](PLy_generate_spi_exceptions.md)
  - [HASHCTL](../H/HASHCTL.md) (PostgreSQL hash table control structure)
  - [PLyExceptionEntry](PLyExceptionEntry.md) (exception entry structure)
  - HASH_ELEM, HASH_BLOBS (hash table flags)
  - PLy_exc_module (exception module definition)
- Called from (representative examples):
  - [PyInit_plpy](PyInit_plpy.md)

## Notes and Other Information
- Creates the spiexceptions submodule within the main plpy module
- Manually increments reference count for the exception module due to PyModule_AddObject behavior
- Establishes a hash table for efficient lookup of SPI exceptions by error code
- Sets up the base exception hierarchy: Error (base), Fatal, and SPIError
- The hash table uses PostgreSQL error codes as keys and PLyExceptionEntry structures as values
- Critical for enabling proper error handling between PostgreSQL and Python code
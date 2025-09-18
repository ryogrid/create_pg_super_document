# PLy_create_exception

## Location
src/pl/plpython/plpy_plpymodule.c: 211 - 239

## Overview
PLy_create_exception is a utility function that creates Python exception objects and adds them to a specified module, handling proper reference counting for PL/Python exception management.

## Definition
```c
static PyObject *PLy_create_exception(char *name, PyObject *base, PyObject *dict, const char *modname, PyObject *mod)
```

## Detailed Description
This function creates a new Python exception class using the Python C API and properly integrates it into a module. It handles the complex reference counting requirements when adding objects to Python modules, ensuring that exception objects remain valid throughout the PL/Python session. The function creates the exception with an optional base class and dictionary, adds it to the specified module, and manages reference counts to prevent premature garbage collection.

## Parameters / Member Variables
- `name`: Full qualified name of the exception (e.g., "plpy.Error")
- `base`: Base exception class (can be NULL for default base)
- `dict`: Dictionary of additional attributes for the exception (can be NULL)
- `modname`: Name to use when adding the exception to the module
- `mod`: PyObject pointer to the module where the exception will be added

## Dependencies
- Functions called/Symbols referenced:
  - PyErr_NewException (Python C API)
  - PLy_elog
  - Py_INCREF (Python C API)
  - PyModule_AddObject (Python C API)
- Called from (representative examples):
  - [PLy_add_exceptions](PLy_add_exceptions.md) (multiple calls for Error, Fatal, SPIError)
  - [PLy_generate_spi_exceptions](PLy_generate_spi_exceptions.md)

## Notes and Other Information
- Carefully manages Python reference counting due to PyModule_AddObject behavior
- Adds an extra reference count for permanent storage of the exception object
- Used to create both base exceptions (Error, Fatal, SPIError) and specific SPI exceptions
- Essential for establishing the exception hierarchy that allows PL/Python code to catch database errors
- The function is defensive about reference counting to prevent memory management issues
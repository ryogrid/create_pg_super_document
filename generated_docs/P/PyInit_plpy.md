# PyInit_plpy

## Location
[src/pl/plpython/plpy_plpymodule.c:129-142](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L129-L142)

## Overview
PyInit_plpy is the Python module initialization function for the plpy module, which is the Python interface to PostgreSQL's PL/Python language extension.

## Definition

```c
PyMODINIT_FUNC
PyInit_plpy(void)
```
## Detailed Description
This function serves as the entry point for initializing the plpy Python module when it is imported. It follows Python's C extension module initialization protocol by creating a new Python module object using the PLy_module definition and adding PostgreSQL-specific exceptions to it. The function is marked with PyMODINIT_FUNC to ensure proper linkage and visibility, especially on Windows platforms where it enables DLL export functionality.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - PyModule_Create (Python C API)
  - [PLy_add_exceptions](PLy_add_exceptions.md)
  - PLy_module (module definition structure)
- Called from (representative examples):
  - [PLy_initialize](PLy_initialize.md)
  - Referenced in PLPY_PLPYMODULE_H

## Notes and Other Information
- Must have external linkage due to PyMODINIT_FUNC requirements for Windows DLL export
- This is a standard Python C extension initialization function following Python's module initialization protocol
- The function creates the base module structure and then delegates exception setup to PLy_add_exceptions
- Returns NULL on failure (following Python C API conventions)
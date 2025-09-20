# PLy_init_interp

## Location
[src/pl/plpython/plpy_main.c:138-157](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L138-L157)

## Overview
Initializes the Python interpreter environment and global data structures specifically for PL/Python execution within PostgreSQL.

## Definition

```c
static void
PLy_init_interp(void)
```
## Detailed Description
This function sets up the Python interpreter's global namespace for PL/Python operations. It imports the Python '__main__' module, establishes global dictionaries for PL/Python execution, and creates a safe globals dictionary accessible as 'GD' within PL/Python procedures. The function is designed to be called only once during the initialization process and handles the critical task of preparing the Python execution environment within PostgreSQL's context.

## Parameters / Member Variables


## Dependencies
- Functions called/Symbols referenced:
  - PyImport_AddModule (Python C API function to get/create module)
  - PyErr_Occurred (Python error checking)
  - PLy_elog (PL/Python error logging)
  - Py_INCREF (Python reference counting)
  - PyModule_GetDict (get module's dictionary)
  - PyDict_New (create new Python dictionary)
  - PyDict_SetItemString (set dictionary item)
  - Py_DECREF (Python reference counting)
- Called from (representative examples):
  - [PLy_initialize](PLy_initialize.md) (single caller)

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c at lines 133-155
- Creates PLy_interp_globals and PLy_interp_safe_globals for PL/Python execution
- The 'GD' dictionary provides a global data sharing mechanism between PL/Python functions
- Function is marked as static, indicating internal use within the compilation unit
- Includes comprehensive error checking with appropriate cleanup via reference counting
- Part of the PL/Python initialization sequence that must complete successfully before executing Python procedures
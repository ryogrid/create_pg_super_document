# PLy_init_plpy

## Location
[src/pl/plpython/plpy_plpymodule.c:143-174](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_plpymodule.c#L143-L174)

## Overview
PLy_init_plpy initializes the PL/Python module's internal structures and sets up the plpy module in Python's main namespace for use within PL/Python functions.

## Definition
```c
void PLy_init_plpy(void)
```

## Detailed Description
This function performs comprehensive initialization of the PL/Python environment by setting up all the necessary Python types and making the plpy module available in the Python interpreter's main namespace. It initializes various PL/Python-specific types (plans, results, subtransactions, cursors), creates the plpy module, and imports it into the main Python namespace so it can be accessed by PL/Python functions. The function ensures that PL/Python functions can access PostgreSQL functionality through the plpy interface.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_plan_init_type](PLy_plan_init_type.md)
  - [PLy_result_init_type](PLy_result_init_type.md)  
  - [PLy_subtransaction_init_type](PLy_subtransaction_init_type.md)
  - [PLy_cursor_init_type](PLy_cursor_init_type.md)
  - PyModule_Create (Python C API)
  - PyImport_AddModule (Python C API)
  - PyModule_GetDict (Python C API)
  - PyDict_SetItemString (Python C API)
  - PLy_elog
  - PLy_module (module definition structure)
- Called from (representative examples):
  - [PLy_initialize](PLy_initialize.md)
  - Referenced in PLPY_PLPYMODULE_H

## Notes and Other Information
- Initializes all PL/Python-specific types before making the module available
- Adds the plpy module to Python's main namespace so PL/Python functions can import and use it
- Includes error handling to ensure the plpy module is successfully imported
- The function bridges PostgreSQL's internal functionality with Python's module system
- Critical for enabling PL/Python functions to interact with the database through the plpy interface
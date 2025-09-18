# PLy_initialize

## Location
src/pl/plpython/plpy_main.c: 95 - 137

## Overview
Performs one-time setup of the PL/Python procedural language extension, including Python interpreter initialization and conflict detection with other Python versions.

## Definition


## Detailed Description
This function handles the initialization of the PL/Python environment within a PostgreSQL session. It performs critical safety checks to ensure only one Python major version is loaded per session, then proceeds with initializing the Python interpreter, importing required modules, and setting up PL/Python-specific infrastructure. The function uses a static boolean flag to ensure initialization occurs only once per session, making it safe to call multiple times.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - PyImport_AppendInittab (Python C API function)
  - PyInit_plpy (PL/Python module initializer)
  - Py_Initialize (Python interpreter initialization)
  - PyImport_ImportModule (Python module import)
  - PLy_init_interp (PL/Python interpreter setup)
  - PLy_init_plpy (PL/Python module setup)
  - PyErr_Occurred (Python error checking)
  - PLy_elog (PL/Python error logging)
  - init_procedure_caches (procedure cache initialization)
- Called from (representative examples):
  - plpython3_validator
  - plpython3_call_handler
  - plpython3_inline_handler

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c at lines 90-131
- Uses a static 'inited' flag to prevent multiple initialization
- Includes version conflict detection using plpython_version_bitmask_ptr
- Initializes global variables like explicit_subtransactions and PLy_execution_contexts
- Part of PostgreSQL's procedural language infrastructure for Python support
- The function is marked as static, indicating it's only used within the same compilation unit
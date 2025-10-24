# PLy_initialize

## Location
[src/pl/plpython/plpy_main.c:95-137](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_main.c#L95-L137)

## Overview
Performs one-time setup of the PL/Python procedural language extension, including Python interpreter initialization and conflict detection with other Python versions.

## Definition

```c
static void
PLy_initialize(void)
```
## Detailed Description
This function handles the initialization of the PL/Python environment within a PostgreSQL session. It performs critical safety checks to ensure only one Python major version is loaded per session, then proceeds with initializing the Python interpreter, importing required modules, and setting up PL/Python-specific infrastructure. The function uses a static boolean flag to ensure initialization occurs only once per session, making it safe to call multiple times.

## Parameters / Member Variables



## Dependencies
- Functions called/Symbols referenced:
  - ereport (for error reporting)
  - PyImport_AppendInittab (Python C API function)
  - [PyInit_plpy](PyInit_plpy.md) (PL/Python module initializer)
  - Py_Initialize (Python interpreter initialization)
  - PyImport_ImportModule (Python module import)
  - [PLy_init_interp](PLy_init_interp.md) (PL/Python interpreter setup)
  - [PLy_init_plpy](PLy_init_plpy.md) (PL/Python module setup)
  - PyErr_Occurred (Python error checking)
  - PLy_elog (PL/Python error logging)
  - [init_procedure_caches](../i/init_procedure_caches.md) (procedure cache initialization)
- Called from (representative examples):
  - [plpython3_validator](../p/plpython3_validator.md)
  - [plpython3_call_handler](../p/plpython3_call_handler.md)
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md)

## Notes and Other Information
- Located in src/pl/plpython/plpy_main.c at lines 90-131
- Uses a static 'inited' flag to prevent multiple initialization
- Includes version conflict detection using plpython_version_bitmask_ptr
- Initializes global variables like explicit_subtransactions and PLy_execution_contexts
- Part of PostgreSQL's procedural language infrastructure for Python support
- The function is marked as static, indicating it's only used within the same compilation unit

## Simplified Source

```c
static void PLy_initialize(void) {
    static bool inited = false;

    // Check for Python version conflicts - only one major version allowed
    if (*plpython_version_bitmask_ptr != (1 << PY_MAJOR_VERSION))
        ereport(FATAL,
                (errmsg("multiple Python libraries are present in session"),
                 errdetail("Only one Python major version can be used in one session.")));

    // Skip if already initialized
    if (inited)
        return;

    // Initialize Python interpreter and PL/Python infrastructure
    PyImport_AppendInittab("plpy", PyInit_plpy);
    Py_Initialize();
    PyImport_ImportModule("plpy");
    PLy_init_interp();
    PLy_init_plpy();

    // Check for initialization errors
    if (PyErr_Occurred())
        PLy_elog(FATAL, "untrapped error in initialization");

    // Initialize procedure caches and global state
    init_procedure_caches();
    explicit_subtransactions = NIL;
    PLy_execution_contexts = NULL;

    inited = true;
}
```
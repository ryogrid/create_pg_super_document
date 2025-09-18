# PLy_procedure_compile

## Location
src/pl/plpython/plpy_procedure.c: 352 - 402

## Overview
Compiles PL/Python source code into executable Python bytecode, setting up the runtime environment with global and static dictionaries for function execution.

## Definition
```c
void PLy_procedure_compile(PLyProcedure *proc, const char *src)
```

## Detailed Description
This function takes raw PL/Python source code and transforms it into executable Python bytecode stored in the procedure object. It creates a copy of the global interpreter namespace for isolation, sets up static data dictionary (SD) for persistent data between calls, mangles the source code to create a proper Python function definition, compiles the source using PyRun_String, and finally compiles a function call template for efficient execution. The function handles both named procedures and anonymous code blocks with appropriate error reporting.

## Parameters / Member Variables
- `proc`: Pointer to PLyProcedure structure to store the compiled code and runtime environment
- `src`: C string containing the PL/Python source code to be compiled

## Dependencies
- Functions called/Symbols referenced:
  - PyDict_Copy (Python dictionary duplication)
  - PyDict_New (Python dictionary creation)
  - PyDict_SetItemString (Python dictionary manipulation)
  - [PLy_procedure_munge_source](PLy_procedure_munge_source.md) (source code transformation)
  - [MemoryContextStrdup](../M/MemoryContextStrdup.md) (memory-context aware string duplication)
  - PyRun_String (Python source compilation)
  - Py_CompileString (Python expression compilation)
  - PLy_elog (PL/Python error reporting)
  - Py_DECREF (Python reference counting)
- Called from (representative examples):
  - [PLy_procedure_create](PLy_procedure_create.md) (during procedure creation)
  - [plpython3_inline_handler](../p/plpython3_inline_handler.md) (for inline code blocks)

## Notes and Other Information
- Creates isolated execution environment by copying global interpreter state
- Sets up SD (Static Data) dictionary for persistent data between function calls
- Uses PLy_procedure_munge_source to transform raw SQL function body into proper Python function definition
- Stores both the compiled bytecode (proc->code) and mangled source (proc->src) for runtime and debugging
- Compiles a function call template for efficient repeated execution
- Provides distinct error messages for named functions vs anonymous code blocks
- Critical for the PL/Python compilation pipeline, transforming SQL function definitions into executable Python code
- The compiled bytecode is stored in the procedure cache for reuse across multiple calls
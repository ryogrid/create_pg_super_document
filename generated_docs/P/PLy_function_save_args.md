# PLy_function_save_args

## Location
[src/pl/plpython/plpy_exec.c:498-543](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L498-L543)

## Overview
PLy_function_save_args creates a snapshot of the current argument values in a procedure's global namespace, enabling restoration of argument state for recursive calls and set-returning function iterations.

## Definition


## Detailed Description
This function addresses a legacy design decision where PL/Python function arguments are made available through the procedure's global namespace. To handle recursive function calls and set-returning functions that may be interleaved, the current argument values must be saved and restored. The function performs:

1. **Memory Allocation**: Allocates a PLySavedArgs structure in the procedure's memory context, sized to hold references to all arguments
2. **Arguments List Preservation**: Saves the "args" list from the global namespace (the positional arguments)
3. **Trigger Data Preservation**: For trigger procedures, additionally saves the "TD" (trigger data) object
4. **Named Arguments Preservation**: Saves all named arguments from the global namespace if the procedure uses named parameters
5. **Reference Management**: Properly increments Python object reference counts to prevent premature garbage collection

The saved argument state can later be restored using PLy_function_restore_args to maintain correct argument visibility across nested calls.

## Parameters / Member Variables
- : PLyProcedure structure containing the procedure metadata, global namespace, and memory context

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryContextAllocZero](../M/MemoryContextAllocZero.md) (PostgreSQL memory allocation)
  - PyDict_GetItemString (Python dictionary access)
  - Py_XINCREF (Python reference counting)
  - [PLySavedArgs](PLySavedArgs.md) structure for holding saved state
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (for set-returning function iteration state)
  - [PLy_global_args_push](PLy_global_args_push.md) (for recursive call management)

## Notes and Other Information
- Addresses the legacy design where arguments are accessible via global variables in PL/Python
- Memory allocation uses the procedure's context to ensure proper cleanup
- Handles both regular functions and trigger procedures (saving "TD" for triggers)
- Supports both positional and named argument preservation
- Reference counting ensures saved Python objects remain valid
- Returns a PLySavedArgs structure that can be passed to PLy_function_restore_args
- File location: src/pl/plpython/plpy_exec.c:498-543
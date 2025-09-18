# PLy_function_restore_args

## Location
src/pl/plpython/plpy_exec.c: 544 - 583

## Overview
PLy_function_restore_args restores previously saved argument values to a procedure's global namespace and properly cleans up the saved argument structure.

## Definition


## Detailed Description
This function is the counterpart to PLy_function_save_args, responsible for restoring argument state that was previously saved. It handles the complete restoration process including:

1. **Named Arguments Restoration**: Iterates through all named arguments and restores them to their original positions in the procedure's global namespace dictionary
2. **Arguments List Restoration**: Restores the "args" list (positional arguments) to the global namespace
3. **Trigger Data Restoration**: For trigger procedures, restores the "TD" (trigger data) object to the global namespace
4. **Reference Count Management**: Properly decrements Python object reference counts as objects are restored, preventing memory leaks
5. **Memory Cleanup**: Frees the PLySavedArgs structure after restoration is complete

This function is essential for maintaining correct argument visibility when returning from recursive function calls or continuing set-returning function iterations where multiple calls may be interleaved.

## Parameters / Member Variables
- : PLyProcedure structure containing the procedure metadata and global namespace to restore into
- : PLySavedArgs structure containing the previously saved argument state to restore

## Dependencies
- Functions called/Symbols referenced:
  - PyDict_SetItemString (Python dictionary operations for restoration)
  - Py_DECREF (Python reference counting)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation)
  - [PLySavedArgs](PLySavedArgs.md) and PLyProcedure structures
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (for set-returning function state restoration)
  - [PLy_global_args_pop](PLy_global_args_pop.md) (for recursive call cleanup)

## Notes and Other Information
- Complements PLy_function_save_args to provide complete argument state management
- Handles both positional and named argument restoration
- Supports trigger procedures with "TD" object restoration
- Properly manages Python object reference counts to prevent memory leaks
- Automatically cleans up the saved argument structure after restoration
- Critical for correct behavior of recursive calls and interleaved set-returning functions
- File location: src/pl/plpython/plpy_exec.c:544-583
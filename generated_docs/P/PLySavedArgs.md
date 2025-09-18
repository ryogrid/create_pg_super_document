# PLySavedArgs

## Location
src/pl/plpython/plpy_procedure.h: 15 - 22

## Overview
PLySavedArgs is a structure used in PostgreSQL's PL/Python extension to save function arguments for outer recursion levels or set-returning functions, enabling proper context management during nested function calls.

## Definition


## Detailed Description
PLySavedArgs implements a linked-list structure that preserves function arguments and context information when PL/Python functions are called recursively or when dealing with set-returning functions. This structure ensures that each function call level maintains its own argument context, preventing argument corruption during nested executions. The structure stores both positional arguments (args) and named arguments (namedargs), along with trigger-specific data (td) when applicable.

## Parameters / Member Variables
- : Pointer to the next PLySavedArgs structure in the linked list, enabling stacking of multiple argument contexts
- : Python object containing the "args" element from the globals dictionary, representing positional arguments
- : Python object containing the "TD" (trigger data) element from globals dictionary, used only for trigger functions
- : Integer specifying the length of the namedargs array, indicating how many named arguments are stored
- : Flexible array member containing Python objects representing named function arguments

## Dependencies
- Functions called/Symbols referenced:
  - FLEXIBLE_ARRAY_MEMBER (for variable-length array implementation)
- Called from (representative examples):
  - [PLySRFState](PLySRFState.md) (contains PLySavedArgs for set-returning function state)
  - [PLy_function_save_args](PLy_function_save_args.md) (creates and populates PLySavedArgs structures)
  - [PLy_function_restore_args](PLy_function_restore_args.md) (restores arguments from PLySavedArgs)
  - [PLy_function_drop_args](PLy_function_drop_args.md) (deallocates PLySavedArgs structures)
  - [PLy_global_args_push](PLy_global_args_push.md) (pushes arguments onto the stack)
  - [PLy_global_args_pop](PLy_global_args_pop.md) (pops arguments from the stack)
  - [PLyProcedure](PLyProcedure.md) (contains PLySavedArgs for procedure context)

## Notes and Other Information
- This structure is essential for maintaining proper argument isolation in recursive PL/Python function calls
- The flexible array member allows for efficient storage of variable numbers of named arguments
- Memory management is critical when working with this structure due to Python object references
- Used primarily in src/pl/plpython/plpy_exec.c for argument management functions
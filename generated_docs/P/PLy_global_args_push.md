# PLy_global_args_push

## Location
src/pl/plpython/plpy_exec.c: 613 - 642

## Overview
Saves existing argument values for a PLpython procedure and pushes them onto a stack to enable recursive function calls by preserving the outer call's argument context.

## Definition
```c
static void PLy_global_args_push(PLyProcedure *proc)
```

## Detailed Description
This function manages the argument stack for PLpython procedures to support recursive function calls. When a PLpython function calls itself or another PLpython function recursively, the current argument values need to be preserved so they can be restored when the inner call completes. The function checks if there's already an active call (calldepth > 0), and if so, saves the current arguments using PLy_function_save_args() and pushes them onto the procedure's argument stack. It always increments the call depth counter to track recursion levels.

## Parameters / Member Variables
- `proc`: Pointer to PLyProcedure structure representing the Python procedure, containing call depth information and argument stack

## Dependencies
- Functions called/Symbols referenced:
  - [PLy_function_save_args](PLy_function_save_args.md) (saves current argument state)
  - [PLyProcedure](PLyProcedure.md) (procedure structure type)
  - [PLySavedArgs](PLySavedArgs.md) (saved arguments structure type)
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (at src/pl/plpython/plpy_exec.c:70)
  - [PLy_exec_trigger](PLy_exec_trigger.md) (at src/pl/plpython/plpy_exec.c:359)

## Notes and Other Information
- This is a static function internal to plpy_exec.c
- Must be paired with exactly one call to PLy_global_args_pop() to maintain stack consistency
- Only saves arguments if calldepth > 0, meaning there's already an active call
- Critical for supporting recursive PLpython function calls without corrupting argument values
- The function maintains a linked list stack structure using the 'next' field in PLySavedArgs
- Once proc->argstack or proc->calldepth is modified, the function must complete without error to maintain consistency
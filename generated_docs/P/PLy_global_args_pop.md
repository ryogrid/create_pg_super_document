# PLy_global_args_pop

## Location
src/pl/plpython/plpy_exec.c: 643 - 680

## Overview
Restores the previous argument values when exiting a recursive PLpython function call by popping saved arguments from the procedure's argument stack.

## Definition
```c
static void PLy_global_args_pop(PLyProcedure *proc)
```

## Detailed Description
This function is the counterpart to PLy_global_args_push(), responsible for restoring the argument state when returning from a recursive PLpython function call. It decrements the call depth and, if there are saved arguments on the stack (calldepth > 1), pops the most recent saved arguments and restores them to the procedure's global context. The function is designed to be failure-safe by adjusting the call stack state before performing operations that could fail, ensuring stack consistency even if memory restoration encounters errors.

## Parameters / Member Variables
- `proc`: Pointer to PLyProcedure structure representing the Python procedure, containing call depth information and argument stack

## Dependencies
- Functions called/Symbols referenced:
  - Assert (assertion macro for debugging)
  - [PLy_function_restore_args](PLy_function_restore_args.md) (restores argument values from saved state)
  - [PLyProcedure](PLyProcedure.md) (procedure structure type)
  - [PLySavedArgs](PLySavedArgs.md) (saved arguments structure type)
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (at src/pl/plpython/plpy_exec.c:258, 287)
  - [PLy_exec_trigger](PLy_exec_trigger.md) (at src/pl/plpython/plpy_exec.c:423)

## Notes and Other Information
- This is a static function internal to plpy_exec.c
- Must be called exactly once for each successful PLy_global_args_push() call
- Uses assertions to verify call stack consistency (calldepth > 0, proper stack state)
- Designed to be failure-safe: adjusts stack state before potentially failing operations
- When exiting the outermost call level (calldepth becomes 0), no argument restoration is needed
- Previously used to clean up named arguments from globals dict, but this optimization was removed as unnecessary
- Critical for maintaining proper argument isolation between recursive function calls
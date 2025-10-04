# PLy_function_drop_args

## Location
[src/pl/plpython/plpy_exec.c:584-612](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L584-L612)

## Overview
Frees a PLySavedArgs struct and all its Python object references without restoring the values to the global argument context.

## Definition

```c
static void
PLy_function_drop_args(PLySavedArgs *savedargs)
```
## Detailed Description
This function is responsible for cleaning up a PLySavedArgs structure by properly decrementing reference counts for all Python objects it contains and then freeing the structure itself. It is used when saved arguments need to be discarded rather than restored, typically during error cleanup or when exiting from set-returning functions. The function ensures proper Python memory management by calling Py_XDECREF on all stored Python objects before deallocating the C structure.

## Parameters / Member Variables
- `*savedargs`: Pointer to PLySavedArgs structure to be freed, containing saved function arguments and trigger data

## Dependencies
- Functions called/Symbols referenced:
  - Py_XDECREF (Python C API macro for safely decrementing reference counts)
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [PLySavedArgs](PLySavedArgs.md) (structure type definition)
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (at src/pl/plpython/plpy_exec.c:276)
  - [plpython_srf_cleanup_callback](../p/plpython_srf_cleanup_callback.md) (at src/pl/plpython/plpy_exec.c:690)

## Notes and Other Information
- This is a static function internal to plpy_exec.c
- Uses Py_XDECREF instead of Py_DECREF to safely handle NULL pointers
- Part of the PLpython memory management system for handling nested function calls and set-returning functions
- The function handles both named arguments array and special Python objects ('args' and 'TD' for trigger data)
- Critical for preventing memory leaks when Python function execution is terminated abnormally

## Simplified Source

```c
static void
PLy_function_drop_args(PLySavedArgs *savedargs)
{
    int i;

    // Drop references for all named arguments
    for (i = 0; i < savedargs->nargs; i++) {
        Py_XDECREF(savedargs->namedargs[i]);
    }

    // Drop references to "args" list and "TD" trigger data
    Py_XDECREF(savedargs->args);
    Py_XDECREF(savedargs->td);

    // Free the saved arguments structure
    pfree(savedargs);
}
```
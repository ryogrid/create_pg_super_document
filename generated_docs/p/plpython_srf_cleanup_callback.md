# plpython_srf_cleanup_callback

## Location
[src/pl/plpython/plpy_exec.c:681-694](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L681-L694)

## Overview
Memory context deletion callback function that cleans up PLySRFState resources when a set-returning PLpython function is terminated early due to error or incomplete execution.

## Definition
```c
static void plpython_srf_cleanup_callback(void *arg)
```

## Detailed Description
This callback function is registered with PostgreSQL's memory context system to ensure proper cleanup of PLpython set-returning function (SRF) state when the function execution is terminated prematurely. It handles cleanup of Python iterator objects and saved function arguments that would otherwise leak memory if the SRF doesn't run to completion normally. The function is essential for preventing memory leaks in scenarios where SRFs are cancelled, encounter errors, or are simply not consumed fully by the caller.

## Parameters / Member Variables
- `arg`: void pointer that is cast to PLySRFState*, containing the SRF state to be cleaned up

## Dependencies
- Functions called/Symbols referenced:
  - Py_XDECREF (Python C API macro for safely decrementing reference counts)
  - [PLy_function_drop_args](../P/PLy_function_drop_args.md) (frees saved function arguments)
  - [PLySRFState](../P/PLySRFState.md) (set-returning function state structure)
- Called from (representative examples):
  - [PLy_exec_function](../P/PLy_exec_function.md) (registered as callback at src/pl/plpython/plpy_exec.c:84)

## Notes and Other Information
- This is a static function internal to plpy_exec.c
- Registered as a memory context deletion callback to ensure cleanup even during abnormal termination
- Uses Py_XDECREF instead of Py_DECREF to safely handle potentially NULL iterator objects
- Essential for preventing memory leaks when set-returning functions don't complete normally
- Part of PostgreSQL's memory management system integration with Python reference counting
- Handles both Python object cleanup (iterator) and PostgreSQL memory cleanup (saved arguments)
- Sets cleaned-up pointers to NULL to prevent double-free scenarios

## Simplified Source

```c
static void
plpython_srf_cleanup_callback(void *arg)
{
    PLySRFState *srfstate = (PLySRFState *) arg;

    // Release Python iterator reference if we still have one
    Py_XDECREF(srfstate->iter);
    srfstate->iter = NULL;

    // Drop any saved function arguments
    if (srfstate->savedargs)
        PLy_function_drop_args(srfstate->savedargs);
    srfstate->savedargs = NULL;
}
```
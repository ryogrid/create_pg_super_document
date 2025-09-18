# PLySRFState

## Location
[src/pl/plpython/plpy_exec.c:27-32](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/pl/plpython/plpy_exec.c#L27-L32)

## Overview
PLySRFState is a state structure used to manage the execution context for Python set-returning functions (SRFs) in PostgreSQLs PL/Python language extension.

## Definition
```c
typedef struct PLySRFState
{
	PyObject   *iter;			/* Python iterator producing results */
	PLySavedArgs *savedargs;	/* function argument values */
	MemoryContextCallback callback; /* for releasing refcounts when done */
} PLySRFState;
```

## Detailed Description
PLySRFState maintains the state necessary for PostgreSQL set-returning functions written in Python. Set-returning functions can be called multiple times to return a series of values, and this structure preserves the execution context between calls.

The structure is allocated in the functions multi-call memory context on the first call to a set-returning function and persists across all subsequent calls until the function completes or is terminated early. It manages both the Python iterator that generates results and the saved function arguments that may need to be restored between calls.

The structure includes a memory context callback mechanism to ensure proper cleanup of Python object references, preventing memory leaks when function execution is terminated early due to errors or incomplete iteration.

## Parameters / Member Variables
- `iter`: Python iterator object that produces the sequence of return values for the set-returning function
- `savedargs`: Pointer to saved function argument values that need to be restored in the Python globals dict for subsequent calls, ensuring consistency when multiple evaluations are interleaved
- `callback`: Memory context callback structure used to register cleanup function that releases Python object references when the memory context is destroyed

## Dependencies
- Functions called/Symbols referenced:
  - [PLySavedArgs](PLySavedArgs.md)
  - [MemoryContextCallback](../M/MemoryContextCallback.md)
  - PyObject (Python C API)
- Called from (representative examples):
  - [PLy_exec_function](PLy_exec_function.md) (primary usage for managing SRF state)
  - [plpython_srf_cleanup_callback](../p/plpython_srf_cleanup_callback.md) (cleanup operations)

## Notes and Other Information
- The structure is allocated using MemoryContextAllocZero in the multi-call memory context to ensure zero-initialization
- A cleanup callback is immediately registered after allocation to handle early termination scenarios
- The iter field holds a Python iterator created from the functions return value using PyObject_GetIter()
- The savedargs field is used to restore function arguments between calls when multiple SRF evaluations are interleaved
- Proper reference counting management is critical - Python objects are decremented during cleanup to prevent memory leaks
- The structure is only used for set-returning functions (proc->is_setof == true), not for regular functions
- Memory management follows PostgreSQLs memory context system, with automatic cleanup when the context is reset or deleted
# LookupParallelWorkerFunction

## Location
[src/backend/access/transam/parallel.c:1629-1652](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/parallel.c#L1629-L1652)

## Overview
LookupParallelWorkerFunction resolves parallel worker entry point functions by name, supporting both core PostgreSQL functions and external library functions.

## Definition
```c
static parallel_worker_main_type LookupParallelWorkerFunction(const char *libraryname, const char *funcname)
```

## Detailed Description
LookupParallelWorkerFunction provides a mechanism to resolve parallel worker entry point functions by their string names rather than function pointers. This indirection is necessary because function addresses may differ between processes, particularly on platforms using EXEC_BACKEND (like Windows) or when dealing with dynamically loaded libraries.

The function operates using a two-tiered lookup strategy:

1. **Core functions**: When the library name is "postgres", the function searches the `InternalParallelWorkers` array, which contains mappings between function names and their addresses for built-in parallel worker functions.

2. **External functions**: For any other library name, the function uses `load_external_function()` to dynamically load the specified function from the named library.

This design allows parallel operations to specify worker functions as strings that can be safely transmitted across process boundaries and resolved to the correct function address in each worker process, regardless of address space layout differences.

## Parameters / Member Variables
- `libraryname`: The name of the library containing the function ("postgres" for core functions, or actual library name for external functions)
- `funcname`: The name of the parallel worker function to look up

## Dependencies
- Functions called/Symbols referenced:
  - lengthof (macro for array size)
  - [load_external_function](../l/load_external_function.md) (dynamic library loading)
  - InternalParallelWorkers (global array of core parallel worker functions)
  - strcmp (string comparison)
  - elog (error logging)
- Called from (representative examples):
  - [ParallelWorkerMain](../P/ParallelWorkerMain.md) (during worker initialization)

## Notes and Other Information
- Declared as `static` - only visible within parallel.c
- Returns type `parallel_worker_main_type` which is a function pointer type
- The `InternalParallelWorkers` array contains entries with `fn_name` and `fn_addr` fields
- Throws an ERROR if a core function ("postgres" library) is not found in the internal array
- Uses `load_external_function` with `strict=true` for external functions
- The design addresses portability issues with function addresses across process boundaries
- Future consideration mentioned for unifying core and external function loading mechanisms
- Critical for the parallel query infrastructure's ability to spawn workers with specific entry points
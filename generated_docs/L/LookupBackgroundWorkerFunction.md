# LookupBackgroundWorkerFunction

## Location
[src/backend/postmaster/bgworker.c:1262-1295](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L1262-L1295)

## Overview
Looks up and possibly loads a background worker entry point function by name, handling both internal PostgreSQL functions and external library functions.

## Definition
static bgworker_main_type LookupBackgroundWorkerFunction(const char *libraryname, const char *funcname)

## Detailed Description
This function resolves string-based function names to actual function pointers for background worker entry points. It serves as a critical component in the background worker system's ability to pass function references across process boundaries. The function handles two distinct cases: internal PostgreSQL functions (library name "postgres") which are looked up in the InternalBGWorkers array, and external functions which are dynamically loaded from shared libraries.

The design addresses the fundamental problem that function addresses cannot be directly passed between processes, especially on platforms using EXEC_BACKEND or when dealing with dynamically loaded libraries where the same function may be loaded at different addresses in different processes.

## Parameters / Member Variables
- `libraryname`: Name of the library containing the function ("postgres" for internal functions)
- `funcname`: Name of the function to look up

## Dependencies
- Functions called/Symbols referenced:
  - strcmp (for string comparison)
  - lengthof (macro for array size)
  - elog (for error reporting)
  - [load_external_function](../l/load_external_function.md) (for dynamic library loading)
  - InternalBGWorkers array access
  - bgworker_main_type typedef
- Called from (representative examples):
  - [BackgroundWorkerMain](../B/BackgroundWorkerMain.md)
  - [BackgroundWorkerHandle](../B/BackgroundWorkerHandle.md) (indirect reference)

## Notes and Other Information
- Static function (internal to bgworker.c)
- Handles cross-process function name resolution problem
- Internal functions use InternalBGWorkers lookup table for efficiency
- External functions are dynamically loaded using load_external_function()
- Throws ERROR if internal function name is not found (programming error)
- Returns bgworker_main_type function pointer
- Future consideration mentioned for unifying internal/external function loading
- Located in src/backend/postmaster/bgworker.c:1262-1295
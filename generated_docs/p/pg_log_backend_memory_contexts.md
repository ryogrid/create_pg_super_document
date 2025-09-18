# pg_log_backend_memory_contexts

## Location
src/backend/utils/adt/mcxtfuncs.c: 144 - 187

## Overview
SQL-callable function that signals a backend or auxiliary process to log its memory contexts to the server log.

## Definition
```c
Datum pg_log_backend_memory_contexts(PG_FUNCTION_ARGS)
```

## Detailed Description
This function allows administrators to trigger memory context logging for a specific PostgreSQL process identified by its PID. It sends a PROCSIG_LOG_MEMORY_CONTEXT signal to the target process, which will cause that process to log its memory context information to the server log file on the next interrupt check.

The function performs validation to ensure the target PID corresponds to a valid PostgreSQL backend or auxiliary process. It uses BackendPidGetProc() for regular backends and AuxiliaryPidGetProc() for auxiliary processes like background workers. If the target process terminates between validation and signal sending, the function handles this gracefully with a warning rather than an error.

This mechanism is designed for debugging memory usage issues and is typically used when a process is consuming excessive memory. The actual logging occurs asynchronously when the target process next checks for interrupts.

## Parameters / Member Variables
- `pid`: Integer parameter (PG_GETARG_INT32(0)) - Process ID of the target PostgreSQL process to signal
- Returns boolean: true if signal was sent successfully, false otherwise

## Dependencies
- Functions called/Symbols referenced:
  - BackendPidGetProc
  - AuxiliaryPidGetProc  
  - GetNumberFromPGProc
  - [SendProcSignal](../S/SendProcSignal.md)
  - PROCSIG_LOG_MEMORY_CONTEXT
  - [PGPROC](../P/PGPROC.md) (struct type)
  - ProcNumber (type)
  - INVALID_PROC_NUMBER
- Called from (representative examples):
  - Direct SQL function calls by administrators
  - Monitoring scripts and tools

## Notes and Other Information
- By default, only superusers can execute this function due to potential for denial of service
- Additional roles can be granted execute permission with explicit GRANT statements
- The function returns warnings rather than errors for invalid PIDs to support batch operations
- Memory contexts are logged asynchronously when the target process checks for interrupts
- Used primarily for debugging memory leaks and excessive memory consumption
- The target process must be a PostgreSQL server process (backend or auxiliary)
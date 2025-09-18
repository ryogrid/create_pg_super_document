# BackgroundWorkerMain

## Location
[src/backend/postmaster/bgworker.c:723-861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L723-L861)

## Overview
The main entry point function for background worker processes that handles initialization, signal setup, and execution of user-defined worker code.

## Definition


## Detailed Description
This function serves as the primary entry point for all background worker processes in PostgreSQL. It performs comprehensive initialization including memory management, signal handling, error recovery setup, and database infrastructure initialization. The function copies the worker configuration from startup data, sets up appropriate signal handlers based on whether the worker needs database access, establishes error handling with setjmp/longjmp, initializes PostgreSQL's process infrastructure, and finally invokes the user-defined worker function.

The function handles two distinct types of background workers: those that require database connections (with full signal handling) and those that don't (with minimal signal handling). It ensures proper cleanup and error reporting in case of exceptions during worker execution.

## Parameters / Member Variables
- : Serialized BackgroundWorker structure containing worker configuration
- : Length of the startup data (must equal sizeof(BackgroundWorker))

## Dependencies
- Functions called/Symbols referenced:
  - elog, Assert (error handling and assertions)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md), memcpy (memory management)
  - [MemoryContextDelete](../M/MemoryContextDelete.md) (cleanup)
  - init_ps_display (process display)
  - SetProcessingMode (processing mode setup)
  - [pg_usleep](../p/pg_usleep.md) (authentication delay)
  - [pqsignal](../p/pqsignal.md) (signal handler setup)
  - [StatementCancelHandler](../S/StatementCancelHandler.md), procsignal_sigusr1_handler, FloatExceptionHandler, bgworker_die
  - InitializeTimeouts, BackgroundWorkerUnblockSignals
  - sigsetjmp, HOLD_INTERRUPTS, EmitErrorReport, proc_exit (error handling)
  - InitProcess, BaseInit (PostgreSQL initialization)
  - [LookupBackgroundWorkerFunction](../L/LookupBackgroundWorkerFunction.md) (worker function lookup)
- Constants referenced:
  - B_BG_WORKER, InitProcessing
  - BGWORKER_BACKEND_DATABASE_CONNECTION
  - Various signal constants (SIGINT, SIGTERM, SIGHUP, etc.)
  - SIG_IGN, SIG_DFL
- Global variables accessed:
  - PostmasterContext, MyBgworkerEntry, MyBackendType
  - PostAuthDelay, PG_exception_stack, error_context_stack
- Called from:
  - child_process_kind (process launcher)

## Notes and Other Information
- This is a public function that serves as the standard entry point for all background workers
- Sets up different signal handling depending on whether the worker needs database access
- Uses setjmp/longjmp for exception handling to ensure proper cleanup on errors
- Does not call InitPostgres - workers must explicitly connect to databases via BackgroundWorkerInitializeConnection()
- Performs proper memory context cleanup by deleting PostmasterContext
- Applies PostAuthDelay if configured for security
- Workers exit with status 0 on normal completion, status 1 on error
- The function does not return under normal circumstances - it exits via proc_exit()
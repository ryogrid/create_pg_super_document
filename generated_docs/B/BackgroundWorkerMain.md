# BackgroundWorkerMain

## Location
[src/backend/postmaster/bgworker.c:723-861](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/postmaster/bgworker.c#L723-L861)

## Overview
The main entry point function for background worker processes that handles initialization, signal setup, and execution of user-defined worker code.

## Definition

```c
struct in shared memory.  We must do this
	 * before we can use LWLocks or access any shared memory.
	 */
	InitProcess();
```
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
  - [InitializeTimeouts](../I/InitializeTimeouts.md), BackgroundWorkerUnblockSignals
  - sigsetjmp, HOLD_INTERRUPTS, EmitErrorReport, proc_exit (error handling)
  - [InitProcess](../I/InitProcess.md), BaseInit (PostgreSQL initialization)
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

## Simplified Source

```c
void BackgroundWorkerMain(char *startup_data, size_t startup_data_len) {
    sigjmp_buf local_sigjmp_buf;
    BackgroundWorker *worker;
    bgworker_main_type entrypt;

    // Validate and copy worker configuration
    if (startup_data == NULL)
        elog(FATAL, "unable to find bgworker entry");
    worker = MemoryContextAlloc(TopMemoryContext, sizeof(BackgroundWorker));
    memcpy(worker, startup_data, sizeof(BackgroundWorker));

    // Initialize process state
    MyBgworkerEntry = worker;
    MyBackendType = B_BG_WORKER;
    init_ps_display(worker->bgw_name);
    SetProcessingMode(InitProcessing);

    // Set up signal handlers based on worker type
    if (worker->bgw_flags & BGWORKER_BACKEND_DATABASE_CONNECTION) {
        pqsignal(SIGINT, StatementCancelHandler);
        pqsignal(SIGUSR1, procsignal_sigusr1_handler);
        pqsignal(SIGFPE, FloatExceptionHandler);
    } else {
        pqsignal(SIGINT, SIG_IGN);
        pqsignal(SIGUSR1, SIG_IGN);
        pqsignal(SIGFPE, SIG_IGN);
    }
    pqsignal(SIGTERM, bgworker_die);

    // Set up error recovery
    if (sigsetjmp(local_sigjmp_buf, 1) != 0) {
        error_context_stack = NULL;
        HOLD_INTERRUPTS();
        BackgroundWorkerUnblockSignals();
        EmitErrorReport();
        proc_exit(1);
    }
    PG_exception_stack = &local_sigjmp_buf;

    // Initialize PostgreSQL infrastructure
    InitProcess();
    BaseInit();

    // Look up and execute worker function
    entrypt = LookupBackgroundWorkerFunction(worker->bgw_library_name,
                                             worker->bgw_function_name);
    entrypt(worker->bgw_main_arg);

    // Normal exit
    proc_exit(0);
}
```
# proc_exit_prepare

## Location
src/backend/storage/ipc/ipc.c: 165 - 227

## Overview
The proc_exit_prepare function performs the core cleanup operations during PostgreSQL process termination, serving as the shared implementation between normal process exit and emergency atexit handling.

## Definition


## Detailed Description
proc_exit_prepare implements the critical phase of process termination where all cleanup operations must be performed. It sets the proc_exit_inprogress flag to prevent recursive exit attempts, clears all pending interrupt flags to avoid interference during cleanup, resets error handling contexts, calls shmem_exit() for shared memory cleanup, and then executes all registered on_proc_exit callbacks in reverse order. The function is designed to be idempotent and safe to call multiple times, as it may be invoked both through normal proc_exit() and through the emergency atexit_callback().

## Parameters / Member Variables
- : Exit status code passed to cleanup callbacks and shared memory exit routines

## Dependencies
- Functions called/Symbols referenced:
  - shmem_exit
  - elog (for debugging)
- Called from (representative examples):
  - proc_exit (normal termination path)
  - atexit_callback (emergency termination path)

## Notes and Other Information
- Declared as static, only accessible within the same source file
- Sets proc_exit_inprogress flag to prevent recursive termination attempts
- Clears interrupt flags (InterruptPending, ProcDiePending, QueryCancelPending) to prevent interference
- Resets error context stack and debug_query_string to prevent callbacks from failing
- Executes callbacks in reverse registration order using decremental index
- Designed to be safe for multiple invocations (second call will have nothing to do)
- Includes protection against infinite loops by decrementing callback index before execution
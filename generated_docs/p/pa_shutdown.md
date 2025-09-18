# pa_shutdown

## Location
src/backend/replication/logical/applyparallelworker.c: 844 - 856

## Overview
pa_shutdown is a cleanup function that ensures proper communication with the leader apply worker and resource cleanup when a parallel apply worker process terminates.

## Definition
```c
static void pa_shutdown(int code, Datum arg)
```

## Detailed Description
This function serves as an exit callback for parallel apply workers in PostgreSQL's logical replication system. It performs two critical cleanup operations when a worker process is shutting down.

First, it sends a signal (PROCSIG_PARALLEL_APPLY_MESSAGE) to the leader apply worker process to ensure the leader attempts to read from the error queue one final time. This is a safety mechanism that guards against unclean exits where the worker process terminates without properly sending an ErrorResponse message, such as when code directly calls proc_exit.

Second, it explicitly detaches from the dynamic shared memory (DSM) segment, which triggers any registered on_dsm_detach callbacks. This ensures proper resource cleanup and maintains consistency in the shared memory management system.

## Parameters / Member Variables
- `code`: Exit code passed by the process exit mechanism (not used in function body)
- `arg`: Datum containing pointer to the DSM segment to detach from

## Dependencies
- Functions called/Symbols referenced:
  - [SendProcSignal](../S/SendProcSignal.md)
  - PROCSIG_PARALLEL_APPLY_MESSAGE
  - INVALID_PROC_NUMBER
  - [dsm_detach](../d/dsm_detach.md)
  - [DatumGetPointer](../D/DatumGetPointer.md)
  - MyLogicalRepWorker (global variable)
- Called from (representative examples):
  - [ParallelApplyWorkerMain](../P/ParallelApplyWorkerMain.md) (registered as exit callback)

## Notes and Other Information
- This is a static function, accessible only within applyparallelworker.c
- Registered as an exit callback using before_shmem_exit or on_proc_exit
- The function design follows PostgreSQL's pattern for cleanup callbacks that take (int code, Datum arg) parameters
- Critical for maintaining communication protocol between leader and parallel workers during shutdown
- Part of PostgreSQL's robust error handling and resource management for parallel logical replication processes
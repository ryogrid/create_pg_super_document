# on_shmem_exit

## Location
[src/backend/storage/ipc/ipc.c:365-393](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L365-L393)

## Overview
Registers callbacks to perform low-level shutdown operations (such as releasing PGPROC resources) that run after before_shmem_exit callbacks but before on_proc_exit callbacks.

## Definition
```c
void on_shmem_exit(pg_on_exit_callback function, Datum arg)
```

## Detailed Description
The `on_shmem_exit` function is part of PostgreSQL's hierarchical shutdown mechanism, specifically designed for registering callbacks that handle low-level resource cleanup. These callbacks execute after the higher-level cleanup performed by `before_shmem_exit` but before the final process-level cleanup of `on_proc_exit`. This staging ensures that shared memory-related resources are properly released in the correct order during process termination.

The function manages an internal list (`on_shmem_exit_list`) of registered callbacks and ensures the atexit mechanism is properly initialized to trigger the cleanup sequence.

## Parameters / Member Variables
- `function`: A callback function of type `pg_on_exit_callback` to be executed during low-level cleanup
- `arg`: A `Datum` argument that will be passed to the callback function when invoked

## Dependencies
- Functions called/Symbols referenced:
  - `MAX_ON_EXITS` (constant defining maximum number of exit callbacks)
  - [atexit_callback](../a/atexit_callback.md) (the actual cleanup function registered with atexit)
  - `ereport` (error reporting)
  - `atexit` (standard C library function)

- Called from (representative examples):
  - [PGReserveSemaphores](../P/PGReserveSemaphores.md) (semaphore resource management)
  - [PGSharedMemoryCreate](../P/PGSharedMemoryCreate.md) (shared memory initialization)
  - [AutoVacWorkerMain](../A/AutoVacWorkerMain.md) (autovacuum worker processes)
  - `InitProcess` (process initialization in shared memory)
  - [InitBufferPoolAccess](../I/InitBufferPoolAccess.md) (buffer pool management)
  - `dsm_postmaster_startup` (dynamic shared memory)
  - [WalReceiverMain](../W/WalReceiverMain.md) (WAL receiver process)

## Notes and Other Information
- This is the middle tier of PostgreSQL's three-tiered exit callback system: before_shmem_exit → on_shmem_exit → on_proc_exit
- Callbacks registered here typically handle shared memory resource cleanup, process slot deallocation, and IPC resource management
- Like `before_shmem_exit`, it has a hard limit of `MAX_ON_EXITS` callbacks and will report a FATAL error if exceeded
- The `atexit_callback_setup` flag ensures the C library atexit handler is registered only once across all exit callback types
- Critical for proper cleanup of PGPROC slots, shared memory segments, semaphores, and other low-level PostgreSQL resources
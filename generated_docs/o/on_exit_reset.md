# on_exit_reset

## Location
[src/backend/storage/ipc/ipc.c:416-431](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L416-L431)

## Overview
Clears all registered exit callbacks (before_shmem_exit, on_shmem_exit, and on_proc_exit) to prevent child processes from executing parent process cleanup routines.

## Definition
```c
void on_exit_reset(void)
```

## Detailed Description
The `on_exit_reset` function is a critical component of PostgreSQL's process forking mechanism. When the postmaster forks a new backend process, the child process inherits all the parent's registered exit callbacks. However, these callbacks are intended for the parent process and should not be executed by the child process when it exits. This function solves this problem by clearing all exit callback registrations, ensuring that child processes start with clean exit callback lists.

The function resets the indices for all three types of exit callbacks to zero, effectively clearing their respective callback lists, and also resets any dynamic shared memory detach callbacks.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - [reset_on_dsm_detach](../r/reset_on_dsm_detach.md) (clears dynamic shared memory detach callbacks)
  - Modifies `before_shmem_exit_index`, `on_shmem_exit_index`, and `on_proc_exit_index` (global indices)

- Called from (representative examples):
  - [InitPostmasterChild](../I/InitPostmasterChild.md) (initialization of child processes)
  - `PG_END_ENSURE_ERROR_CLEANUP` (error cleanup macro)

## Notes and Other Information
- Essential for proper process isolation in PostgreSQL's multi-process architecture
- Prevents child processes from accidentally executing cleanup routines intended for the parent postmaster
- Called immediately after forking to ensure child processes have clean exit callback state
- Resets all three tiers of exit callbacks: before_shmem_exit, on_shmem_exit, and on_proc_exit
- Also handles dynamic shared memory (DSM) cleanup callback reset via `reset_on_dsm_detach`
- Critical for preventing resource cleanup conflicts between parent and child processes
- Part of PostgreSQL's fork-safe resource management system
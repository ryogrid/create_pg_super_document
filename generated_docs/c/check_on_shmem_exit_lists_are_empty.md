# check_on_shmem_exit_lists_are_empty

## Location
[src/backend/storage/ipc/ipc.c:432-439](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L432-L439)

## Overview
A debugging function that verifies no shared memory cleanup handlers have been registered prematurely in the current process.

## Definition
```c
void check_on_shmem_exit_lists_are_empty(void)
```

## Detailed Description
The `check_on_shmem_exit_lists_are_empty` function serves as a debugging and validation mechanism to ensure that shared memory exit callbacks are not registered at inappropriate times during process initialization. It checks both the `before_shmem_exit_index` and `on_shmem_exit_index` counters to verify they are zero, indicating that no callbacks have been registered yet.

If either index is non-zero, the function reports a FATAL error, indicating that cleanup handlers were registered prematurely. This helps catch programming errors where exit callbacks might be registered before the process is properly initialized or in contexts where they shouldn't exist.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - `elog` (error reporting with FATAL level)
  - Checks `before_shmem_exit_index` and `on_shmem_exit_index` (global indices)

- Called from (representative examples):
  - [BackendInitialize](../B/BackendInitialize.md) (during backend process initialization)
  - `PG_END_ENSURE_ERROR_CLEANUP` (error cleanup macro)

## Notes and Other Information
- This is primarily a debugging and validation function to catch programming errors
- Reports FATAL errors if cleanup handlers are found to be registered prematurely
- Does not check DSM (Dynamic Shared Memory) detach state, as noted in the source comment
- Used during process initialization to ensure clean startup state
- Helps maintain the integrity of PostgreSQL's exit callback system by preventing premature registration
- Part of PostgreSQL's defensive programming practices to catch initialization order issues early
- The FATAL error level ensures the process terminates immediately if premature registration is detected

## Simplified Source

```c
// Simplified version of check_on_shmem_exit_lists_are_empty
void check_on_shmem_exit_lists_are_empty(void) {
    // Check if before_shmem_exit callbacks were registered prematurely
    if (before_shmem_exit_index) {
        elog(FATAL, "before_shmem_exit has been called prematurely");
    }

    // Check if on_shmem_exit callbacks were registered prematurely
    if (on_shmem_exit_index) {
        elog(FATAL, "on_shmem_exit has been called prematurely");
    }

    // Note: DSM detach state checking is not needed given above checks
}
```

Key simplifications made:
- Added clear comments explaining each validation check
- Maintained the original logic flow and error handling
- Preserved the essential debugging functionality
- Clarified the purpose of each index check
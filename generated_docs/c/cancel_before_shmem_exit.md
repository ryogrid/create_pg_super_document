# cancel_before_shmem_exit

## Location
[src/backend/storage/ipc/ipc.c:394-415](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L394-L415)

## Overview
Removes a previously-registered before_shmem_exit callback, specifically targeting the most recently added entry in strict LIFO order.

## Definition
```c
void cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg)
```

## Detailed Description
The `cancel_before_shmem_exit` function provides a mechanism to remove callbacks that were previously registered with `before_shmem_exit`. It operates under the assumption that callers add and remove temporary callbacks in strict Last-In-First-Out (LIFO) order, so it only checks and removes the most recently registered callback. This design choice ensures predictable behavior and prevents accidental removal of callbacks registered by other parts of the system.

If the specified callback is not the latest entry or doesn't exist, the function reports an ERROR, indicating a programming error in the calling code.

## Parameters / Member Variables
- `function`: The callback function of type `pg_on_exit_callback` to be removed
- `arg`: The `Datum` argument associated with the callback to be removed

## Dependencies
- Functions called/Symbols referenced:
  - `elog` (error reporting)
  - Accesses `before_shmem_exit_list` and `before_shmem_exit_index` (internal data structures)

- Called from (representative examples):
  - `PG_END_ENSURE_ERROR_CLEANUP` (error cleanup macro in ipc.h)

## Notes and Other Information
- This function enforces strict LIFO ordering for callback removal - only the most recently registered callback can be canceled
- The function performs exact matching on both the function pointer and argument value
- If the callback to be canceled is not the latest entry, an ERROR is logged with details about the mismatched callback
- Commonly used in error cleanup scenarios where temporary callbacks need to be removed when operations complete successfully
- Part of PostgreSQL's error-safe resource management system, often used within PG_ENSURE_ERROR_CLEANUP blocks
- The LIFO restriction prevents complex callback management issues that could arise from arbitrary removal of callbacks

## Simplified Source

```c
void cancel_before_shmem_exit(pg_on_exit_callback function, Datum arg) {
    // Remove the most recently registered callback (LIFO order only)
    if (before_shmem_exit_index > 0 &&
        before_shmem_exit_list[before_shmem_exit_index - 1].function == function &&
        before_shmem_exit_list[before_shmem_exit_index - 1].arg == arg) {
        --before_shmem_exit_index;
    } else {
        elog(ERROR, "before_shmem_exit callback is not the latest entry");
    }
}
```
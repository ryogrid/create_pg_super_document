# before_shmem_exit

## Location
[src/backend/storage/ipc/ipc.c:337-364](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/ipc.c#L337-L364)

## Overview
Registers early cleanup callbacks to perform user-level cleanup operations (such as transaction abort) before PostgreSQL begins shutting down low-level subsystems.

## Definition

```c
void
before_shmem_exit(pg_on_exit_callback function, Datum arg)
```
## Detailed Description
The  function allows PostgreSQL components to register cleanup callbacks that need to execute before shared memory and other low-level subsystems are torn down during process exit. This is particularly important for maintaining data consistency and proper cleanup of higher-level operations like transactions, namespace cleanup, and replication state.

The function maintains an internal list of registered callbacks () and ensures that the standard C library  callback is properly set up to trigger the cleanup sequence when the process terminates.

## Parameters / Member Variables
- : A callback function of type  to be executed during cleanup
- : A  argument that will be passed to the callback function when it's invoked

## Dependencies
- Functions called/Symbols referenced:
  -  (constant defining maximum number of exit callbacks)
  -  (the actual cleanup function registered with atexit)
  -  (error reporting)
  -  (standard C library function)

- Called from (representative examples):
  -  (parallel query worker initialization)
  -  (two-phase commit preparation)
  -  (backend initialization)
  -  (background checkpointer process)
  -  (replication slot management)
  -  (statistics subsystem)

## Notes and Other Information
- The function has a hard limit of  callbacks that can be registered
- If the limit is exceeded, a FATAL error is reported with error code 
- The  flag ensures the C library atexit handler is registered only once
- This is part of PostgreSQL's hierarchical shutdown mechanism, providing cleanup at a higher level than
- Callbacks registered here typically handle transaction-level or session-level cleanup before lower-level resource cleanup begins

## Simplified Source

```c
// Simplified version of before_shmem_exit
void before_shmem_exit(pg_on_exit_callback function, Datum arg) {
    // Core logic step 1: Check if we have room for another callback
    if (before_shmem_exit_index >= MAX_ON_EXITS) {
        ereport(FATAL,
                (errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
                 errmsg_internal("out of before_shmem_exit slots")));
    }

    // Core logic step 2: Store the callback function and its argument
    before_shmem_exit_list[before_shmem_exit_index].function = function;
    before_shmem_exit_list[before_shmem_exit_index].arg = arg;

    // Core logic step 3: Increment the index for next callback
    ++before_shmem_exit_index;

    // Core logic step 4: Ensure atexit handler is registered (one-time setup)
    if (!atexit_callback_setup) {
        atexit(atexit_callback);
        atexit_callback_setup = true;
    }
}
```

Key simplifications made:
- Added descriptive comments for each core logic step
- Maintained the essential error checking and callback registration logic
- Preserved the one-time atexit setup mechanism
- Focused on the main execution path without removing critical functionality
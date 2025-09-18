# before_shmem_exit

## Location
src/backend/storage/ipc/ipc.c: 337 - 364

## Overview
Registers early cleanup callbacks to perform user-level cleanup operations (such as transaction abort) before PostgreSQL begins shutting down low-level subsystems.

## Definition


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
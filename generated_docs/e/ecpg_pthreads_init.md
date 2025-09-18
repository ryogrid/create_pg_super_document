# ecpg_pthreads_init

## Location
src/interfaces/ecpg/ecpglib/connect.c: 30 - 35

## Overview
Ensures thread-safe initialization of the pthread thread-specific data key used by the ECPG library for managing per-thread database connections.

## Definition
```c
void ecpg_pthreads_init(void)
```

## Detailed Description
This function provides a thread-safe way to initialize the pthread infrastructure needed for ECPG's per-thread connection management. It uses `pthread_once()` to ensure that the `ecpg_actual_connection_init()` function is called exactly once across all threads in the process, regardless of how many times `ecpg_pthreads_init()` itself is called. This is crucial for proper initialization of the `actual_connection_key` which allows different threads to maintain separate current database connections.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - `pthread_once` (POSIX threads library function)
  - `[ecpg_actual_connection_init](ecpg_actual_connection_init.md)` (initialization callback function)
- Called from (representative examples):
  - `[ecpg_get_connection_nr](ecpg_get_connection_nr.md)` (to ensure key is initialized before use)
  - `ecpg_get_connection` (connection management)
  - `[ECPGconnect](../E/ECPGconnect.md)` (when establishing new connections)
  - `ecpg_do_prologue` (before SQL execution)

## Notes and Other Information
- This is a public function (no static qualifier) that can be called from multiple ECPG library modules
- Uses `actual_connection_key_once` (a `pthread_once_t`) to ensure single initialization
- Essential for thread safety in multi-threaded ECPG applications
- Called proactively by various ECPG functions to ensure the threading infrastructure is ready
- Part of PostgreSQL's embedded SQL (ECPG) preprocessor library infrastructure
# StartAutovacuumWorker

## Location
src/backend/postmaster/postmaster.c: 3962 - 4052

## Overview
StartAutovacuumWorker creates and manages autovacuum worker processes, handling the full lifecycle from process creation to backend registration and error handling.

## Definition


## Detailed Description
StartAutovacuumWorker is responsible for creating autovacuum worker processes in PostgreSQL. Unlike simple auxiliary processes, autovacuum workers require more complex setup including backend registration, cancel key generation, and slot assignment. The function first checks if the system is in a suitable state to accept autovacuum connections using canAcceptConnections(). 

If conditions are favorable, it generates a random cancel key for the worker process, allocates a Backend structure, and assigns a postmaster child slot. The actual process creation is delegated to StartChildProcess() with the B_AUTOVAC_WORKER type. Upon successful creation, the worker is registered in the BackendList and in shared memory (on EXEC_BACKEND builds).

The function includes comprehensive error handling - if process creation fails, it cleans up allocated resources and notifies the autovacuum launcher about the failure. This notification mechanism helps prevent rapid retry loops between the launcher and postmaster.

## Parameters / Member Variables
- No parameters (void function)

## Dependencies
- Functions called/Symbols referenced:
  - canAcceptConnections (checks if system can accept new connections)
  - RandomCancelKey (generates random cancel key for security)
  - palloc_extended (memory allocation with no-OOM option)
  - AssignPostmasterChildSlot (assigns child process slot)
  - StartChildProcess (creates the actual worker process)
  - dlist_push_head (adds to backend list)
  - ShmemBackendArrayAdd (shared memory registration on EXEC_BACKEND)
  - ReleasePostmasterChildSlot (cleanup on failure)
  - AutoVacWorkerFailed (notifies launcher of failure)
- Called from (representative examples):
  - process_pm_pmsignal (signal handler for autovac worker requests)

## Notes and Other Information
- Creates Backend structures for proper process tracking and management
- Generates cancel keys for security, even though autovac workers may not need them
- Implements careful resource cleanup on failure to prevent memory leaks
- Includes race condition prevention by checking system state before proceeding
- Uses deferred signaling approach to avoid ping-pong effects with the autovac launcher
- Code structure roughly matches BackendStartup for consistency
- Workers are marked as non-dead_end processes requiring child slots
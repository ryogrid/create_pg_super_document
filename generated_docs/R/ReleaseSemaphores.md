# ReleaseSemaphores

## Location
src/backend/port/posix_sema.c: 240 - 261

## Overview
ReleaseSemaphores is a cleanup function that releases all acquired semaphores during PostgreSQL shutdown or shared memory reinitialization, ensuring proper cleanup of system resources.

## Definition


## Detailed Description
This function serves as the cleanup callback for PostgreSQL's semaphore system, automatically invoked during process shutdown or shared memory reinitialization. It systematically releases all semaphores that were created during the session, with different cleanup strategies for named and unnamed POSIX semaphores.

The function handles cleanup robustly:
- Iterates through all allocated semaphores (tracked by numSems counter)
- Calls PosixSemaphoreKill() for each semaphore to properly destroy them
- For named semaphores, also frees the postmaster-local pointer array
- For unnamed semaphores, uses PG_SEM_REF macro to access semaphore references

The cleanup is designed to be non-fatal - PosixSemaphoreKill() logs errors rather than crashing, allowing the shutdown process to continue even if individual semaphore cleanup fails.

## Parameters / Member Variables
- : Exit status (standard for on_shmem_exit callbacks, unused in implementation)
- : Additional argument (standard for on_shmem_exit callbacks, unused in implementation)

## Dependencies
- Functions called/Symbols referenced:
  - PosixSemaphoreKill (destroys individual semaphores)
  - PG_SEM_REF (macro to reference unnamed semaphores)
  - free (deallocates memory for named semaphore pointers)
  - Global variables: numSems, mySemPointers, sharedSemas

- Called from:
  - Registered as callback by PGReserveSemaphores via on_shmem_exit()
  - Automatically invoked during PostgreSQL shutdown or shared memory reinitialization

## Notes and Other Information
- Static function, only used internally within the POSIX semaphore implementation
- Registered as an on_shmem_exit callback, so it follows that interface signature
- Designed for graceful degradation - continues cleanup even if individual semaphore destruction fails
- Uses conditional compilation to handle both named and unnamed semaphore configurations
- Part of PostgreSQL's platform abstraction layer for semaphore management
- Critical for preventing semaphore leaks that could exhaust system resources
- The cleanup strategy minimizes dependency on potentially corrupted shared memory contents
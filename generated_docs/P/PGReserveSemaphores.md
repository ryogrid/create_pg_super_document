# PGReserveSemaphores

## Location
[src/backend/port/posix_sema.c:196-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/posix_sema.c#L196-L239)

## Overview
PGReserveSemaphores initializes PostgreSQL's semaphore support during postmaster startup or shared memory reinitialization, preparing the system to handle up to a specified maximum number of semaphores.

## Definition


## Detailed Description
This function performs the initial setup required for PostgreSQL's platform-independent semaphore system. It prepares data structures and allocates memory to support semaphore creation, with different strategies for named and unnamed POSIX semaphores.

Key initialization tasks:
- Seeds the semaphore key generator using the data directory's inode number to minimize collisions with other PostgreSQL instances
- Allocates memory for semaphore management structures (approach differs by semaphore type)
- Registers cleanup callback (ReleaseSemaphores) for proper shutdown
- Sets up global variables for semaphore tracking

For named semaphores:
- Allocates a postmaster-local array of sem_t pointers using malloc()
- This design isolates cleanup from potentially corrupted shared memory

For unnamed semaphores:
- Allocates shared memory for PGSemaphoreData structures using ShmemAllocUnlocked()
- Must use unlocked allocation since spinlock protection isn't ready yet

## Parameters / Member Variables
- : Maximum number of semaphores that will be created during the PostgreSQL session

## Dependencies
- Functions called/Symbols referenced:
  - [stat](../s/stat.md) (file system statistics)
  - malloc (memory allocation for named semaphores)
  - [ShmemAllocUnlocked](../S/ShmemAllocUnlocked.md) (shared memory allocation for unnamed semaphores)
  - [PGSemaphoreShmemSize](PGSemaphoreShmemSize.md) (calculate memory requirements)
  - [on_shmem_exit](../o/on_shmem_exit.md) (register cleanup callback)
  - [ReleaseSemaphores](../R/ReleaseSemaphores.md) (cleanup function)
  - ereport/elog (PostgreSQL error reporting)

- Called from:
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md) (PostgreSQL shared memory initialization)

## Notes and Other Information
- Called during postmaster startup or shared memory reinitialization
- Uses the data directory's inode number as a seed for semaphore key generation to avoid conflicts
- The design minimizes dependency on shared memory contents during shutdown
- For unnamed semaphores, must use ShmemAllocUnlocked() due to spinlock initialization ordering
- Automatically registers ReleaseSemaphores as an exit callback for proper cleanup
- Part of PostgreSQL's platform abstraction layer that allows the same interface across different semaphore implementations
- Sets global variables: numSems, maxSems, nextSemKey, mySemPointers/sharedSemas
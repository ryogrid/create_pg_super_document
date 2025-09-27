# PGReserveSemaphores

## Location
[src/backend/port/posix_sema.c:196-239](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/port/posix_sema.c#L196-L239)

## Overview
PGReserveSemaphores initializes PostgreSQL's semaphore support during postmaster startup or shared memory reinitialization, preparing the system to handle up to a specified maximum number of semaphores.

## Definition

```c
struct stat statbuf;
```
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

## Simplified Source

```c
// Simplified version of PGReserveSemaphores
void PGReserveSemaphores(int maxSemas) {
    struct stat statbuf;

    // Step 1: Get data directory stats to generate unique semaphore keys
    if (stat(DataDir, &statbuf) < 0) {
        ereport(FATAL, (errcode_for_file_access(),
                errmsg("could not stat data directory \"%s\": %m", DataDir)));
    }

    // Step 2: Allocate memory for semaphore management
#ifdef USE_NAMED_POSIX_SEMAPHORES
    // For named semaphores: allocate pointer array in postmaster memory
    mySemPointers = (sem_t **) malloc(maxSemas * sizeof(sem_t *));
    if (mySemPointers == NULL) {
        elog(PANIC, "out of memory");
    }
#else
    // For unnamed semaphores: allocate structures in shared memory
    sharedSemas = (PGSemaphore) ShmemAllocUnlocked(PGSemaphoreShmemSize(maxSemas));
#endif

    // Step 3: Initialize global tracking variables
    numSems = 0;
    maxSems = maxSemas;
    nextSemKey = statbuf.st_ino;  // Use inode as key seed

    // Step 4: Register cleanup function for shutdown
    on_shmem_exit(ReleaseSemaphores, 0);
}
```

Key simplifications made:
- Removed detailed comments explaining design rationale
- Consolidated conditional compilation blocks with clear step labels
- Simplified error handling to show only the essential checks
- Added high-level step comments to explain the main phases
- Focused on the core initialization logic flow
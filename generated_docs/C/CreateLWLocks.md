# CreateLWLocks

## Location
[src/backend/storage/lmgr/lwlock.c:453-492](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/lmgr/lwlock.c#L453-L492)

## Overview
Creates and initializes the main LWLock array in shared memory and registers extension LWLock tranches for the current PostgreSQL instance.

## Definition

```c
void
CreateLWLocks(void)
```
## Detailed Description
CreateLWLocks is responsible for setting up the lightweight lock (LWLock) infrastructure in PostgreSQL. It performs two main functions:

1. **Shared Memory Initialization** (only in postmaster process): Allocates shared memory space for the main LWLock array, ensures proper alignment, sets up the dynamic allocation counter for tranches, and calls InitializeLWLocks to initialize all locks.

2. **Extension Tranche Registration** (in all processes): Registers named extension LWLock tranches that were requested during shared_preload_libraries processing.

The function uses the IsUnderPostmaster check to ensure that shared memory allocation only happens once in the postmaster process, while tranche registration happens in every process that calls this function.

## Parameters / Member Variables
This function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - LWLockShmemSize: Calculates required shared memory size
  - ShmemAlloc: Allocates shared memory
  - InitializeLWLocks: Initializes all LWLock structures
  - LWLockRegisterTranche: Registers tranche names for debugging
- Constants used:
  - LWLOCK_PADDED_SIZE: Alignment size for LWLock structures
  - LWTRANCHE_FIRST_USER_DEFINED: Starting ID for user-defined tranches
- Called from:
  - CreateOrAttachShmemStructs: Main shared memory setup function

## Notes and Other Information
- The function ensures proper memory alignment by adjusting the pointer to align with LWLOCK_PADDED_SIZE boundaries
- A dynamic allocation counter is stored just before the first LWLock to track tranche ID allocation
- Extension tranches are registered in all processes, not just the postmaster
- The MainLWLockArray global variable is set to point to the allocated memory region
- This is a critical initialization function that must be called during PostgreSQL startup
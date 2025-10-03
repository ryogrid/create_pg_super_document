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

## Dependencies
- Functions called/Symbols referenced:
  - [LWLockShmemSize](../L/LWLockShmemSize.md): Calculates required shared memory size
  - [ShmemAlloc](../S/ShmemAlloc.md): Allocates shared memory
  - [InitializeLWLocks](../I/InitializeLWLocks.md): Initializes all LWLock structures
  - [LWLockRegisterTranche](../L/LWLockRegisterTranche.md): Registers tranche names for debugging
- Constants used:
  - LWLOCK_PADDED_SIZE: Alignment size for LWLock structures
  - LWTRANCHE_FIRST_USER_DEFINED: Starting ID for user-defined tranches
- Called from:
  - [CreateOrAttachShmemStructs](CreateOrAttachShmemStructs.md): Main shared memory setup function

## Notes and Other Information
- The function ensures proper memory alignment by adjusting the pointer to align with LWLOCK_PADDED_SIZE boundaries
- A dynamic allocation counter is stored just before the first LWLock to track tranche ID allocation
- Extension tranches are registered in all processes, not just the postmaster
- The MainLWLockArray global variable is set to point to the allocated memory region
- This is a critical initialization function that must be called during PostgreSQL startup

## Simplified Source

```c
// Simplified version of CreateLWLocks
void CreateLWLocks(void) {
    // Phase 1: Shared memory setup (only in postmaster process)
    if (!IsUnderPostmaster) {
        // Calculate required space and allocate shared memory
        Size memory_needed = LWLockShmemSize();
        char *memory_ptr = (char *) ShmemAlloc(memory_needed);

        // Reserve space for dynamic tranche counter
        memory_ptr += sizeof(int);

        // Align memory for LWLock array performance
        memory_ptr = align_to_lwlock_boundary(memory_ptr);

        // Set global pointer to the aligned LWLock array
        MainLWLockArray = (LWLockPadded *) memory_ptr;

        // Initialize the counter for dynamic tranche allocation
        int *tranche_counter = get_counter_location(MainLWLockArray);
        *tranche_counter = LWTRANCHE_FIRST_USER_DEFINED;

        // Initialize all LWLocks in the array
        InitializeLWLocks();
    }

    // Phase 2: Register extension tranches (in all processes)
    for (int i = 0; i < NamedLWLockTrancheRequests; i++) {
        LWLockRegisterTranche(NamedLWLockTrancheArray[i].trancheId,
                             NamedLWLockTrancheArray[i].trancheName);
    }
}
```

Key simplifications made:
- Abstracted pointer arithmetic into conceptual helper functions
- Replaced complex alignment calculation with descriptive function name
- Simplified memory layout setup with clearer variable names
- Added phase comments to show the two main responsibilities
- Focused on the logical flow rather than low-level implementation details
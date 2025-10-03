# InitShmemAllocation

## Location
[src/backend/storage/ipc/shmem.c:115-151](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L115-L151)

## Overview
InitShmemAllocation sets up the shared memory space allocation system by initializing the allocation spinlock and aligning the free space to cache line boundaries.

## Definition
```c
void InitShmemAllocation(void)
```

## Detailed Description
InitShmemAllocation is a critical initialization function that prepares PostgreSQL's shared memory allocation subsystem for operation. This function should only be called in the postmaster or a standalone backend process, never in child processes.

The function performs several essential setup tasks:

1. **Spinlock Initialization**: Creates and initializes the ShmemLock spinlock that protects shared memory allocation operations. This uses ShmemAllocUnlocked since regular ShmemAlloc cannot be used yet.

2. **Cache Line Alignment**: Ensures that all future allocations will be aligned to cache line boundaries for optimal memory performance. It calculates the proper alignment for the current free offset in the shared memory segment.

3. **Index Preparation**: Sets up placeholder values for the shared memory index (ShmemIndex), which will be properly initialized later when LWLocks become available.

The function relies on ShmemSegHdr being already initialized by InitShmemAccess and ensures that subsequent memory allocations through ShmemAlloc will be properly synchronized and aligned.

## Parameters / Member Variables

## Dependencies
- Functions called/Symbols referenced:
  - [PGShmemHeader](../P/PGShmemHeader.md) (type)
  - [ShmemAllocUnlocked](../S/ShmemAllocUnlocked.md)
  - [slock_t](../s/slock_t.md) (type)
  - SpinLockInit
  - CACHELINEALIGN (macro)
  - [HTAB](../H/HTAB.md) (type)
- Called from (representative examples):
  - [CreateSharedMemoryAndSemaphores](../C/CreateSharedMemoryAndSemaphores.md)

## Notes and Other Information
- Must be called only in postmaster or standalone backend processes, never in child processes
- Requires InitShmemAccess to have been called first to set up ShmemSegHdr
- The ShmemIndex cannot be fully initialized at this point because LWLocks are not yet available
- Cache line alignment is crucial for performance on modern multi-core systems
- Uses ShmemAllocUnlocked for the initial spinlock allocation since ShmemAlloc is not yet operational
- Located in src/backend/storage/ipc/shmem.c:115-151

## Simplified Source

```c
// Simplified version of InitShmemAllocation
void InitShmemAllocation(void) {
    PGShmemHeader *shmhdr = ShmemSegHdr;
    char *aligned;

    // Validate shared memory header is available
    Assert(shmhdr != NULL);

    // Step 1: Initialize the shared memory allocation spinlock
    // Use unlocked allocation since ShmemAlloc isn't ready yet
    ShmemLock = (slock_t *) ShmemAllocUnlocked(sizeof(slock_t));
    SpinLockInit(ShmemLock);

    // Step 2: Align future allocations to cache line boundaries
    // Calculate aligned position for next allocation
    aligned = (char *) CACHELINEALIGN((char *)shmhdr + shmhdr->freeoffset);
    shmhdr->freeoffset = aligned - (char *)shmhdr;

    // Step 3: Initialize index placeholders
    // ShmemIndex will be set up later when LWLocks are available
    shmhdr->index = NULL;
    ShmemIndex = NULL;
}
```

Key simplifications made:
- Removed detailed comments and consolidated into step-by-step descriptions
- Simplified the cache line alignment calculation for readability
- Clarified the order of operations with numbered steps
- Maintained the essential logic while improving code clarity
- Preserved all critical functionality and assertions
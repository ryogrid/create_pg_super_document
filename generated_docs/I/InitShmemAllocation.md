# InitShmemAllocation

## Location
src/backend/storage/ipc/shmem.c: 115 - 151

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
None - this function takes no parameters.

## Dependencies
- Functions called/Symbols referenced:
  - PGShmemHeader (type)
  - ShmemAllocUnlocked
  - slock_t (type)
  - SpinLockInit
  - CACHELINEALIGN (macro)
  - HTAB (type)
- Called from (representative examples):
  - CreateSharedMemoryAndSemaphores

## Notes and Other Information
- Must be called only in postmaster or standalone backend processes, never in child processes
- Requires InitShmemAccess to have been called first to set up ShmemSegHdr
- The ShmemIndex cannot be fully initialized at this point because LWLocks are not yet available
- Cache line alignment is crucial for performance on modern multi-core systems
- Uses ShmemAllocUnlocked for the initial spinlock allocation since ShmemAlloc is not yet operational
- Located in src/backend/storage/ipc/shmem.c:115-151
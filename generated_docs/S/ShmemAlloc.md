# ShmemAlloc

## Location
src/backend/storage/ipc/shmem.c: 152 - 171

## Overview
ShmemAlloc allocates a max-aligned chunk of memory from the shared memory segment and throws an error if the allocation cannot be satisfied.

## Definition
```c
void *ShmemAlloc(Size size)
```

## Detailed Description
ShmemAlloc is the primary function for allocating memory from PostgreSQL's shared memory segment. It serves as a wrapper around ShmemAllocRaw that provides error handling for allocation failures.

The function ensures maximum alignment for the allocated memory chunk, which is critical for performance and correctness on various hardware architectures. When called, it attempts to allocate the requested amount of memory and returns a pointer to the allocated space.

If the allocation fails (typically due to insufficient shared memory), ShmemAlloc throws a PostgreSQL ERROR with errcode ERRCODE_OUT_OF_MEMORY, providing details about how much memory was requested. This error-throwing behavior makes it suitable for situations where allocation failure should be treated as a fatal condition.

The function assumes that both ShmemLock (for synchronization) and ShmemSegHdr (for memory management metadata) have been properly initialized by previous initialization functions.

## Parameters / Member Variables
- `size`: The number of bytes to allocate from shared memory. Must be of type Size (typically size_t).

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemAllocRaw](ShmemAllocRaw.md)
- Called from (representative examples):
  - [ShmemBackendArrayAllocation](ShmemBackendArrayAllocation.md)
  - [ShmemInitStruct](ShmemInitStruct.md)
  - CreateLWLocks
  - [InitPredicateLocks](../I/InitPredicateLocks.md)
  - InitProcGlobal

## Notes and Other Information
- Always throws ERROR on allocation failure - use ShmemAllocNoError if you need to handle failures gracefully
- Provides maximum alignment for allocated memory chunks
- Requires ShmemLock and ShmemSegHdr to be initialized before use
- The allocated memory is not initialized - callers should zero it if needed
- Part of PostgreSQL's shared memory management subsystem
- Located in src/backend/storage/ipc/shmem.c:152-171
# ShmemAllocNoError

## Location
src/backend/storage/ipc/shmem.c: 172 - 185

## Overview
ShmemAllocNoError allocates a max-aligned chunk from shared memory but returns NULL on allocation failure instead of throwing an error.

## Definition
```c
void *ShmemAllocNoError(Size size)
```

## Detailed Description
ShmemAllocNoError provides the same memory allocation functionality as ShmemAlloc but with different error handling behavior. Instead of throwing a PostgreSQL ERROR when allocation fails, this function returns NULL, allowing the caller to handle allocation failures gracefully.

This function is particularly useful in scenarios where allocation failure should not be treated as a fatal error, or where the caller wants to implement custom error handling or fallback strategies. Like ShmemAlloc, it ensures maximum alignment for the allocated memory chunk.

The function is implemented as a simple wrapper around ShmemAllocRaw, directly returning its result without any error checking or transformation. This makes it a lightweight alternative to ShmemAlloc when error handling needs to be customized.

## Parameters / Member Variables
- `size`: The number of bytes to allocate from shared memory. Must be of type Size (typically size_t).

## Dependencies
- Functions called/Symbols referenced:
  - [ShmemAllocRaw](ShmemAllocRaw.md)
- Called from (representative examples):
  - ShmemInitHash

## Notes and Other Information
- Returns NULL on allocation failure instead of throwing an error
- Provides the same maximum alignment guarantees as ShmemAlloc
- Useful for scenarios requiring custom error handling for allocation failures
- The allocated memory is not initialized - callers should zero it if needed
- Requires ShmemLock and ShmemSegHdr to be initialized before use
- More lightweight than ShmemAlloc since it doesn't include error reporting overhead
- Located in src/backend/storage/ipc/shmem.c:172-185
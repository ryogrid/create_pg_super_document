# ShmemAllocRaw

## Location
[src/backend/storage/ipc/shmem.c:186-237](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/ipc/shmem.c#L186-L237)

## Overview
ShmemAllocRaw is the core shared memory allocation function that performs cache-line-aligned allocation and returns both the allocated memory pointer and the actual allocated size.

## Definition
```c
static void *ShmemAllocRaw(Size size, Size *allocated_size)
```

## Detailed Description
ShmemAllocRaw is the fundamental allocation function underlying PostgreSQL's shared memory management system. It performs the actual memory allocation from the shared memory segment with sophisticated alignment considerations.

The function ensures that all allocated memory is aligned to cache line boundaries rather than just using MAXALIGN. This design decision is based on experience showing that critical data structures split across cache line boundaries can significantly impact performance on modern systems. The cache line alignment helps prevent false sharing and improves cache efficiency in multi-core environments.

The allocation process involves:

1. **Size Alignment**: The requested size is rounded up to cache line alignment using CACHELINEALIGN
2. **Thread Safety**: Uses ShmemLock spinlock to ensure atomic allocation operations
3. **Bounds Checking**: Verifies that the allocation fits within the total shared memory size
4. **Pointer Calculation**: Computes the new memory address based on ShmemBase and current offset
5. **Offset Update**: Updates the free offset in the shared memory header for the next allocation

The function returns NULL if insufficient space is available, allowing callers to handle allocation failures appropriately.

## Parameters / Member Variables
- `size`: The number of bytes to allocate from shared memory (Size type)
- `allocated_size`: Pointer to Size variable that receives the actual number of bytes allocated (including alignment padding)

## Dependencies
- Functions called/Symbols referenced:
  - CACHELINEALIGN (macro)
- Called from (representative examples):
  - [ShmemAlloc](ShmemAlloc.md)
  - [ShmemAllocNoError](ShmemAllocNoError.md)
  - [ShmemInitStruct](ShmemInitStruct.md)

## Notes and Other Information
- Static function - only accessible within shmem.c
- Provides cache line alignment for optimal performance on modern multi-core systems
- Uses spinlock synchronization to ensure thread-safe allocation
- Returns both allocated pointer and actual allocated size (which may be larger due to alignment)
- The actual allocated size includes any padding added for alignment purposes
- Returns NULL on allocation failure without throwing errors
- Assumes ShmemSegHdr, ShmemBase, and ShmemLock are properly initialized
- Located in src/backend/storage/ipc/shmem.c:186-237

## Simplified Source

```c
// Simplified version of ShmemAllocRaw
static void *ShmemAllocRaw(Size size, Size *allocated_size) {
    Size newStart;
    Size newFree;
    void *newSpace;

    // Align size to cache line boundary for optimal performance
    size = CACHELINEALIGN(size);
    *allocated_size = size;

    Assert(ShmemSegHdr != NULL);

    // Thread-safe allocation using spinlock
    SpinLockAcquire(ShmemLock);

    // Calculate new allocation start position
    newStart = ShmemSegHdr->freeoffset;
    newFree = newStart + size;

    // Check if allocation fits within shared memory bounds
    if (newFree <= ShmemSegHdr->totalsize) {
        // Calculate actual memory address and update free offset
        newSpace = (void *) ((char *) ShmemBase + newStart);
        ShmemSegHdr->freeoffset = newFree;
    } else {
        // Insufficient space available
        newSpace = NULL;
    }

    SpinLockRelease(ShmemLock);

    // Verify proper cache line alignment was achieved
    Assert(newSpace == (void *) CACHELINEALIGN(newSpace));

    return newSpace;
}
```

Key simplifications made:
- Removed extensive alignment explanation comments while preserving the logic
- Added clear comments explaining each major operation
- Preserved essential thread safety and bounds checking
- Maintained cache line alignment for performance optimization
- Focused on the core allocation algorithm and error handling
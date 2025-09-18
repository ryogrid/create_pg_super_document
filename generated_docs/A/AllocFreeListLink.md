# AllocFreeListLink

## Location
src/backend/utils/mmgr/aset.c: 122 - 125

## Overview
AllocFreeListLink is a structure used to implement linked lists of free memory chunks in PostgreSQL's allocation set memory management system.

## Definition
```c
typedef struct AllocFreeListLink
{
    MemoryChunk *next;
} AllocFreeListLink;
```

## Detailed Description
AllocFreeListLink is a crucial component of the allocation set's free list management system. When memory is freed using pfree(), if the allocation set maintains a freelist for chunks of that particular size, an AllocFreeListLink structure is used to maintain the linked list of available free chunks. The structure acts as a node in a singly-linked list, where each freed chunk points to the next available chunk of the same size. This design enables efficient reuse of previously allocated memory blocks, reducing the overhead of frequent allocation and deallocation operations.

## Parameters / Member Variables
- `next`: Pointer to the next MemoryChunk in the free list chain

## Dependencies
- Functions called/Symbols referenced:
  - MemoryChunk (structure representing a memory chunk)
- Called from (representative examples):
  - GetFreeListLink (retrieves free list link from memory chunk)
  - [AllocSetContextCreateInternal](AllocSetContextCreateInternal.md) (initializes allocation context with free lists)
  - [AllocSetAlloc](AllocSetAlloc.md) (uses free lists for efficient allocation)
  - [AllocSetFree](AllocSetFree.md) (adds chunks to appropriate free lists)
  - [AllocSetAllocFromNewBlock](AllocSetAllocFromNewBlock.md) (manages free list when creating new blocks)
  - [AllocSetStats](AllocSetStats.md) (reports statistics about free list usage)

## Notes and Other Information
- Essential for the allocation set's free list optimization strategy
- Helps reduce memory fragmentation by reusing freed chunks of the same size
- The free list mechanism improves performance by avoiding system malloc/free calls for frequently allocated/deallocated chunk sizes
- Used extensively throughout the allocation and deallocation process in aset.c
- Part of PostgreSQL's custom memory management that provides better performance characteristics than standard malloc/free
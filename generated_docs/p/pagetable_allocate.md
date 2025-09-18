# pagetable_allocate

## Location
[src/backend/nodes/tidbitmap.c:1494-1521](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/nodes/tidbitmap.c#L1494-L1521)

## Overview
A callback function for allocating memory for hashtable elements in TID bitmaps, supporting both regular memory contexts and dynamic shared memory areas.

## Definition


## Detailed Description
This function serves as a memory allocation callback for pagetable hash structures in TID bitmaps. It implements a dual allocation strategy: when DSA (Dynamic Shared Area) is not available, it uses regular memory context allocation with huge and zero flags. When DSA is available, it allocates from the shared memory area, carefully managing the old pagetable reference to enable proper cleanup. The function wraps the allocated memory in a PTEntryArray structure and returns a pointer to the entry data.

## Parameters / Member Variables
- `pagetable`: Pointer to the hash table structure requiring memory allocation
- `size`: Size of memory to allocate in bytes

## Dependencies
- Functions called/Symbols referenced:
  - [TIDBitmap](../T/TIDBitmap.md) (struct type)
  - [PTEntryArray](../P/PTEntryArray.md) (struct type)
  - [MemoryContextAllocExtended](../M/MemoryContextAllocExtended.md) (function)
  - MCXT_ALLOC_HUGE (constant)
  - MCXT_ALLOC_ZERO (constant)
  - [dsa_allocate_extended](../d/dsa_allocate_extended.md) (function)
  - DSA_ALLOC_HUGE (constant)
  - DSA_ALLOC_ZERO (constant)
  - [dsa_get_address](../d/dsa_get_address.md) (function)
- Called from (representative examples):
  - Used as callback in hash table operations (not directly referenced)

## Notes and Other Information
- Static inline function for performance in memory allocation hot paths
- Supports both shared and non-shared memory allocation strategies
- Carefully manages DSA pagetable references to enable proper memory cleanup
- Uses huge page and zero-initialization flags for optimal performance
- Critical component for TID bitmap memory management in both regular and parallel contexts
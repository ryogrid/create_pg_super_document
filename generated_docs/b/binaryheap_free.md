# binaryheap_free

## Location
[src/common/binaryheap.c:75-89](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/binaryheap.c#L75-L89)

## Overview
Deallocates memory used by a binary heap structure that was previously allocated with binaryheap_allocate.

## Definition
```c
void binaryheap_free(binaryheap *heap)
```

## Detailed Description
The `binaryheap_free` function releases all memory associated with a binary heap that was previously allocated using `binaryheap_allocate`. It uses PostgreSQL's `pfree` function to deallocate the memory block containing both the heap structure and its associated node array. This function should be called when the heap is no longer needed to prevent memory leaks.

## Parameters / Member Variables
- `heap`: Pointer to the binary heap to be deallocated

## Dependencies
- Functions called/Symbols referenced:
  - [pfree](../p/pfree.md) (PostgreSQL memory deallocation function)
  - [binaryheap](binaryheap.md) (struct type)
- Called from (representative examples):
  - [ReorderBufferIterTXNFinish](../R/ReorderBufferIterTXNFinish.md)
  - [BufferSync](../B/BufferSync.md)
  - [restore_toc_entries_parallel](../r/restore_toc_entries_parallel.md)
  - [TopoSort](../T/TopoSort.md)

## Notes and Other Information
- This function deallocates the entire heap structure including both metadata and node storage
- Must only be called on heaps that were allocated with binaryheap_allocate
- After calling this function, the heap pointer becomes invalid and should not be used
- Part of the standard allocation/deallocation pattern in PostgreSQL memory management
- Uses pfree which is PostgreSQL's counterpart to the standard C library's free() function
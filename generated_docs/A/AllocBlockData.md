# AllocBlockData

## Location
src/backend/utils/mmgr/aset.c: 181 - 188

## Overview
AllocBlockData is the header structure for memory blocks in PostgreSQL's allocation set system, containing metadata and pointers that manage individual blocks of memory obtained from malloc().

## Definition
```c
typedef struct AllocBlockData
{
    AllocSet    aset;       /* aset that owns this block */
    AllocBlock  prev;       /* prev block in aset's blocks list, if any */
    AllocBlock  next;       /* next block in aset's blocks list, if any */
    char       *freeptr;    /* start of free space in this block */
    char       *endptr;     /* end of space in this block */
} AllocBlockData;
```

## Detailed Description
AllocBlockData serves as the header structure for memory blocks in the allocation set system. Each block represents a contiguous chunk of memory obtained from the system's malloc() and contains one or more MemoryChunks that are allocated via palloc() and freed via pfree(). The structure maintains essential metadata for block management including ownership information, linked list pointers for block chaining, and free space tracking. MemoryChunks within a block cannot be returned to malloc() individually; instead, they are managed through freelists for efficient reuse. The actual usable space within the block begins at the next alignment boundary after this header structure.

## Parameters / Member Variables
- `aset`: Pointer to the AllocSet (allocation set context) that owns this block
- `prev`: Pointer to the previous AllocBlock in the allocation set's doubly-linked blocks list, or NULL if this is the first block
- `next`: Pointer to the next AllocBlock in the allocation set's doubly-linked blocks list, or NULL if this is the last block
- `freeptr`: Pointer to the start of free (unallocated) space within this block
- `endptr`: Pointer to the end of the total space in this block (marks the block boundary)

## Dependencies
- Functions called/Symbols referenced:
  - AllocSet (typedef for allocation set context pointer)
  - [AllocBlock](AllocBlock.md) (typedef for pointer to this structure)
- Called from (representative examples):
  - ALLOC_BLOCKHDRSZ (macro that calculates the size of this header structure)
  - [AllocBlock](AllocBlock.md) (typedef that creates pointer type to this structure)

## Notes and Other Information
- The header is excluded from the usable allocation space within the block
- Blocks form a doubly-linked list within each allocation set for efficient traversal and management
- The freeptr and endptr fields are crucial for managing space allocation within the block
- Part of PostgreSQL's custom memory management that provides better performance and debugging capabilities than standard malloc/free
- The block size and layout are carefully designed to maintain proper memory alignment
- Used extensively throughout the allocation set implementation for block-based memory management
- The structure enables efficient block coalescing and splitting operations during memory management
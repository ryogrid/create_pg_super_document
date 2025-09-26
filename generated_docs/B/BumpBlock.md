# BumpBlock

## Location
src/backend/utils/mmgr/bump.c: 86 - 99

## Overview
BumpBlock represents a memory block unit obtained from malloc() that serves as the storage container for bump pointer allocations within a BumpContext.

## Definition

```c
struct BumpBlock
{
	dlist_node	node;			/* doubly-linked list of blocks */
#ifdef MEMORY_CONTEXT_CHECKING
	BumpContext *context;		/* pointer back to the owning context */
#endif
	char	   *freeptr;		/* start of free space in this block */
	char	   *endptr;			/* end of space in this block */
};
```
## Detailed Description
BumpBlock is the fundamental storage unit used by the bump allocator memory context. Each block contains a contiguous region of memory obtained from the system malloc(), and allocations within the bump context are satisfied by advancing the freeptr pointer within the current block. When a block becomes full, a new block is allocated and added to the context's block list.

The block maintains pointers to track the free space region (between freeptr and endptr) and integrates with PostgreSQL's doubly-linked list infrastructure for efficient block management. During debug builds, it also maintains a back-pointer to the owning context for validation purposes.

## Parameters / Member Variables
- : dlist_node structure enabling the block to be part of a doubly-linked list of blocks within the BumpContext
- : (Debug builds only) Back-pointer to the BumpContext that owns this block, used for memory context checking and validation
- : Pointer to the start of available/unallocated space within this block - advanced as allocations are made
- : Pointer to the end of the allocated memory region for this block, marking the boundary of available space

## Dependencies
- Functions called/Symbols referenced:
  - dlist_node (for linked list functionality)
  - BumpContext (back-reference in debug builds)
- Called from (representative examples):
  - BumpContextCreate
  - BumpReset  
  - BumpAlloc
  - BumpAllocLarge
  - BumpAllocFromNewBlock
  - BumpBlockInit
  - BumpBlockFree
  - BumpStats
  - BumpCheck

## Notes and Other Information
- Blocks are obtained from the system via malloc() and their size is determined by the BumpContext's block sizing parameters
- The usable space in a block is the region between the end of the BumpBlock header and endptr
- Blocks are never individually freed during normal operation - they are only freed when the entire context is reset or destroyed  
- The freeptr advances monotonically during allocation and only resets when the entire context is reset
- Block sizes typically grow exponentially (up to maxBlockSize) to reduce allocation overhead for large working sets
- The MEMORY_CONTEXT_CHECKING conditional compilation provides additional debugging capabilities in debug builds
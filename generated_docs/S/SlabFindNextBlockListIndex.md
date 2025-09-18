# SlabFindNextBlockListIndex

## Location
src/backend/utils/mmgr/slab.c: 251 - 270

## Overview
SlabFindNextBlockListIndex searches through the slab allocator's blocklists to find the first blocklist containing blocks with available free chunks for allocation.

## Definition
```c
static int32 SlabFindNextBlockListIndex(SlabContext *slab)
```

## Detailed Description
This function implements a search strategy that prioritizes fuller blocks over emptier ones when looking for available memory chunks. It iterates through the blocklists starting from index 1 (since index 0 is reserved for completely full blocks) and returns the index of the first non-empty blocklist found. This prioritization strategy helps consolidate allocations into fuller blocks, increasing the likelihood that mostly-empty blocks will eventually become completely empty and can be freed back to the system.

The function returns 0 if no blocks with free space are found, indicating that a new block needs to be allocated from the operating system.

## Parameters / Member Variables
- `slab`: Pointer to the SlabContext containing the array of blocklists to search

## Dependencies
- Functions called/Symbols referenced:
  - [SlabContext](SlabContext.md) (struct type)
  - SLAB_BLOCKLIST_COUNT (constant)
  - [dlist_is_empty](../d/dlist_is_empty.md) (function to check if doubly-linked list is empty)
  - MemoryChunk (struct type)
- Called from (representative examples):
  - [SlabAlloc](SlabAlloc.md)
  - [SlabFree](SlabFree.md)

## Notes and Other Information
- This is a static function with internal linkage
- The search starts at index 1 because blocklist[0] is reserved for full blocks with no free chunks
- The prioritization of fuller blocks is a key optimization for memory fragmentation reduction
- Returns 0 when no blocks with free space are available, signaling the need for new block allocation
- The function uses PostgreSQL's doubly-linked list implementation (dlist) for efficient list operations
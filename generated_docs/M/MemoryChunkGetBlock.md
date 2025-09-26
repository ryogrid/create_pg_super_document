# MemoryChunkGetBlock

## Location
[src/include/utils/memutils_memorychunk.h:235-242](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/memutils_memorychunk.h#L235-L242)

## Overview
Calculates and returns the pointer to the memory block that contains a given non-external MemoryChunk by using the block offset stored in the chunk's header mask.

## Definition
```c
static inline void *MemoryChunkGetBlock(MemoryChunk *chunk)
```

## Detailed Description
MemoryChunkGetBlock reconstructs the pointer to the memory block that contains a given MemoryChunk. It works by subtracting the block offset (which was stored during chunk initialization with MemoryChunkSetHdrMask) from the chunk's address. This operation reverses the offset calculation that was performed when the chunk was originally set up.

The function is essential for memory management operations that need to access the block structure containing the chunk, such as during deallocation or when updating block metadata. It includes a debug assertion to ensure it's only called on non-external chunks, since external chunks are not part of memory blocks.

## Parameters / Member Variables
- `chunk`: Pointer to the MemoryChunk structure for which to find the containing block (must be non-external)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryChunk](MemoryChunk.md) (structure type)
  - HdrMaskIsExternal (helper macro/function for debug validation)
  - HdrMaskBlockOffset (helper macro/function to extract block offset)
- Called from (representative examples):
  - [AlignedAllocFree](../A/AlignedAllocFree.md) (alignedalloc.c:39)
  - [AlignedAllocRealloc](../A/AlignedAllocRealloc.md) (alignedalloc.c:73)
  - [AlignedAllocGetChunkContext](../A/AlignedAllocGetChunkContext.md) (alignedalloc.c:145)
  - [AlignedAllocGetChunkSpace](../A/AlignedAllocGetChunkSpace.md) (alignedalloc.c:166)
  - [AllocSetFree](../A/AllocSetFree.md) (aset.c:1111)
  - [AllocSetRealloc](../A/AllocSetRealloc.md) (aset.c:1303)
  - [AllocSetGetChunkContext](../A/AllocSetGetChunkContext.md) (aset.c:1445)
  - [AllocSetCheck](../A/AllocSetCheck.md) (aset.c:1678)
  - [BumpCheck](../B/BumpCheck.md) (bump.c:784)
  - [GenerationFree](../G/GenerationFree.md) (generation.c:720)
  - [GenerationRealloc](../G/GenerationRealloc.md) (generation.c:826)
  - [GenerationGetChunkContext](../G/GenerationGetChunkContext.md) (generation.c:958)
  - [GenerationCheck](../G/GenerationCheck.md) (generation.c:1146)
  - [SlabFree](../S/SlabFree.md) (slab.c:712)
  - [SlabRealloc](../S/SlabRealloc.md) (slab.c:835)
  - [SlabGetChunkContext](../S/SlabGetChunkContext.md) (slab.c:871)
  - [SlabGetChunkSpace](../S/SlabGetChunkSpace.md) (slab.c:896)
  - [SlabCheck](../S/SlabCheck.md) (slab.c:1115)

## Notes and Other Information
- This is an inline function for performance efficiency as it's frequently called during memory operations
- Only valid for non-external chunks; external chunks are not part of memory blocks
- The calculation assumes the block offset was correctly stored during chunk initialization
- Used extensively throughout all PostgreSQL memory context implementations
- Essential for block-level operations like updating free space information and chunk validation
- The pointer arithmetic must match the original offset calculation from MemoryChunkSetHdrMask
- Critical for memory context switching and chunk ownership determination
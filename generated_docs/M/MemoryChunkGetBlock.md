# MemoryChunkGetBlock

## Location
src/include/utils/memutils_memorychunk.h: 235 - 242

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
  - MemoryChunk (structure type)
  - HdrMaskIsExternal (helper macro/function for debug validation)
  - HdrMaskBlockOffset (helper macro/function to extract block offset)
- Called from (representative examples):
  - AlignedAllocFree (alignedalloc.c:39)
  - AlignedAllocRealloc (alignedalloc.c:73)
  - AlignedAllocGetChunkContext (alignedalloc.c:145)
  - AlignedAllocGetChunkSpace (alignedalloc.c:166)
  - AllocSetFree (aset.c:1111)
  - AllocSetRealloc (aset.c:1303)
  - AllocSetGetChunkContext (aset.c:1445)
  - AllocSetCheck (aset.c:1678)
  - BumpCheck (bump.c:784)
  - GenerationFree (generation.c:720)
  - GenerationRealloc (generation.c:826)
  - GenerationGetChunkContext (generation.c:958)
  - GenerationCheck (generation.c:1146)
  - SlabFree (slab.c:712)
  - SlabRealloc (slab.c:835)
  - SlabGetChunkContext (slab.c:871)
  - SlabGetChunkSpace (slab.c:896)
  - SlabCheck (slab.c:1115)

## Notes and Other Information
- This is an inline function for performance efficiency as it's frequently called during memory operations
- Only valid for non-external chunks; external chunks are not part of memory blocks
- The calculation assumes the block offset was correctly stored during chunk initialization
- Used extensively throughout all PostgreSQL memory context implementations
- Essential for block-level operations like updating free space information and chunk validation
- The pointer arithmetic must match the original offset calculation from MemoryChunkSetHdrMask
- Critical for memory context switching and chunk ownership determination
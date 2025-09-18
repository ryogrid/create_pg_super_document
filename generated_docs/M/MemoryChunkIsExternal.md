# MemoryChunkIsExternal

## Location
src/include/utils/memutils_memorychunk.h: 204 - 221

## Overview
Determines whether a MemoryChunk is marked as externally managed by checking the external flag in its header mask.

## Definition
```c
static inline bool MemoryChunkIsExternal(MemoryChunk *chunk)
```

## Detailed Description
MemoryChunkIsExternal is a utility function that checks if a given MemoryChunk is externally managed (allocated outside the normal block-based memory allocation system). The function uses the HdrMaskIsExternal helper to examine the external flag bit in the chunk's hdrmask field. It also includes a debug assertion to verify that external chunks maintain the proper MEMORYCHUNK_MAGIC value, which helps detect memory corruption.

External chunks are typically used for large allocations that exceed the normal chunk size limits and are allocated directly from the system (e.g., via malloc) rather than from memory blocks.

## Parameters / Member Variables
- `chunk`: Pointer to the MemoryChunk structure to be tested

## Dependencies
- Functions called/Symbols referenced:
  - MemoryChunk (structure type)
  - HdrMaskIsExternal (helper macro/function)
  - HdrMaskCheckMagic (helper macro/function for debug validation)
- Called from (representative examples):
  - AlignedAllocFree (alignedalloc.c:36)
  - AlignedAllocGetChunkContext (alignedalloc.c:143)
  - AllocSetFree (aset.c:1070)
  - AllocSetRealloc (aset.c:1180)
  - AllocSetGetChunkContext (aset.c:1442)
  - AllocSetGetChunkSpace (aset.c:1470)
  - AllocSetCheck (aset.c:1654)
  - BumpCheck (bump.c:776)
  - GenerationFree (generation.c:702)
  - GenerationRealloc (generation.c:811)
  - GenerationGetChunkContext (generation.c:955)
  - GenerationGetChunkSpace (generation.c:981)
  - GenerationCheck (generation.c:1138)

## Notes and Other Information
- This is an inline function for performance efficiency as it's frequently called during memory operations
- The function includes debug assertions to validate that external chunks maintain their magic number
- Used extensively throughout PostgreSQL's memory management system to determine the appropriate handling for different chunk types
- External chunks require different deallocation logic since they're not part of memory blocks
- The magic number check helps detect memory corruption or invalid chunk access
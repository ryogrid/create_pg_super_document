# MemoryChunkGetValue

## Location
src/include/utils/memutils_memorychunk.h: 222 - 234

## Overview
Extracts the value field from a non-external MemoryChunk's header mask, returning the size or context-specific value that was originally stored by MemoryChunkSetHdrMask.

## Definition
```c
static inline Size MemoryChunkGetValue(MemoryChunk *chunk)
```

## Detailed Description
MemoryChunkGetValue retrieves the value field that was encoded in a MemoryChunk's hdrmask during initialization with MemoryChunkSetHdrMask. This value typically represents the requested allocation size or other context-specific information stored by the memory context implementation. The function includes a debug assertion to ensure it's only called on non-external chunks, since external chunks don't store value information in the same way.

The function uses the HdrMaskGetValue helper to extract the value from the specific bit positions within the 64-bit hdrmask field.

## Parameters / Member Variables
- `chunk`: Pointer to the MemoryChunk structure from which to retrieve the value (must be non-external)

## Dependencies
- Functions called/Symbols referenced:
  - MemoryChunk (structure type)
  - HdrMaskIsExternal (helper macro/function for debug validation)
  - HdrMaskGetValue (helper macro/function to extract value)
- Called from (representative examples):
  - AlignedAllocRealloc (alignedalloc.c:72)
  - AllocSetAlloc (aset.c:1009)
  - AllocSetFree (aset.c:1124)
  - AllocSetRealloc (aset.c:1314)
  - AllocSetGetChunkSpace (aset.c:1482)
  - AllocSetStats (aset.c:1555)
  - AllocSetCheck (aset.c:1666)
  - BumpCheck (bump.c:785)
  - GenerationFree (generation.c:731)
  - GenerationRealloc (generation.c:835)
  - GenerationGetChunkSpace (generation.c:989)
  - GenerationCheck (generation.c:1147)

## Notes and Other Information
- This is an inline function for performance efficiency as it's frequently called during memory operations
- Only valid for non-external chunks; external chunks store their size information differently
- The returned Size type typically represents the originally requested allocation size
- Used extensively throughout PostgreSQL's memory management implementations for chunk size tracking
- Essential for operations like reallocation, deallocation, and memory usage statistics
- The debug assertion helps prevent incorrect usage on external chunks
# MemoryChunkSetHdrMaskExternal

## Location
[src/include/utils/memutils_memorychunk.h:190-203](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/memutils_memorychunk.h#L190-L203)

## Overview
Initializes a MemoryChunk as an externally managed chunk by setting the external flag and recording the memory context method ID in the header mask.

## Definition
```c
static inline void MemoryChunkSetHdrMaskExternal(MemoryChunk *chunk, MemoryContextMethodID methodid)
```

## Detailed Description
MemoryChunkSetHdrMaskExternal is used to mark a MemoryChunk as externally managed, meaning it's not allocated from a standard memory block but rather through direct memory allocation (typically malloc or similar system calls). This function sets up the hdrmask field with:
- MEMORYCHUNK_MAGIC: A magic number identifier
- External flag bit: Indicates this chunk is externally managed
- Method ID: Identifies which memory context implementation manages this chunk

Unlike MemoryChunkSetHdrMask, this function doesn't encode block offset or value information since external chunks are not part of a memory block structure.

## Parameters / Member Variables
- `chunk`: Pointer to the MemoryChunk structure to be marked as external
- `methodid`: Memory context method identifier (must be <= MEMORY_CONTEXT_METHODID_MASK)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryChunk](MemoryChunk.md) (structure type)
  - MemoryContextMethodID (enum type)
  - MEMORY_CONTEXT_METHODID_MASK
  - MEMORYCHUNK_EXTERNAL_BASEBIT
  - MEMORYCHUNK_MAGIC
- Called from (representative examples):
  - [AllocSetAllocLarge](../A/AllocSetAllocLarge.md) (aset.c:727)
  - [BumpAllocLarge](../B/BumpAllocLarge.md) (bump.c:333)
  - [GenerationAllocLarge](../G/GenerationAllocLarge.md) (generation.c:382)

## Notes and Other Information
- This function is used specifically for large allocations that bypass the normal block-based allocation mechanism
- External chunks are typically used when the requested allocation size exceeds the maximum chunk size that can fit within a memory block
- The MEMORYCHUNK_MAGIC value helps identify valid memory chunks and detect corruption
- This is an inline function for performance efficiency
- External chunks require different handling during deallocation since they're not part of a memory block

## Simplified Source

```c
static inline void
MemoryChunkSetHdrMaskExternal(MemoryChunk *chunk,
                              MemoryContextMethodID methodid)
{
    // Validate method ID fits in allocated bits
    Assert((int) methodid <= MEMORY_CONTEXT_METHODID_MASK);

    // Set header mask with magic number, external flag, and method ID
    chunk->hdrmask = MEMORYCHUNK_MAGIC |
                     (((uint64) 1) << MEMORYCHUNK_EXTERNAL_BASEBIT) |
                     methodid;
}
```
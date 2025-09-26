# MemoryChunkSetHdrMask

## Location
[src/include/utils/memutils_memorychunk.h:169-189](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/utils/memutils_memorychunk.h#L169-L189)

## Overview
Sets up the header mask of a MemoryChunk by encoding the block offset, value, and memory context method ID into a single 64-bit header mask field.

## Definition

```c
static inline void
MemoryChunkSetHdrMask(MemoryChunk *chunk, void *block,
					  Size value, MemoryContextMethodID methodid)
```
## Detailed Description
MemoryChunkSetHdrMask is a core function in PostgreSQL's memory management system that initializes the hdrmask field of a MemoryChunk structure. This function packs three essential pieces of information into a single 64-bit value:
- Block offset: The distance between the chunk and its containing memory block
- Value: An arbitrary size or context-specific value
- Method ID: Identifies which memory context implementation is managing this chunk

The function performs several validation checks to ensure the parameters are within valid ranges and then combines them using bit manipulation operations at specific bit positions defined by the MEMORYCHUNK_*_BASEBIT constants.

## Parameters / Member Variables
- : Pointer to the MemoryChunk structure whose hdrmask will be set
- : Pointer to the memory block that contains this chunk (must be MAXALIGN'd)
- : Size or context-specific value to store (must be <= MEMORYCHUNK_MAX_VALUE)
- : Memory context method identifier (must be <= MEMORY_CONTEXT_METHODID_MASK)

## Dependencies
- Functions called/Symbols referenced:
  - [MemoryChunk](MemoryChunk.md) (structure type)
  - MemoryContextMethodID (enum type)
  - MEMORYCHUNK_BLOCKOFFSET_MASK
  - MEMORYCHUNK_MAX_VALUE  
  - MEMORY_CONTEXT_METHODID_MASK
  - MEMORYCHUNK_BLOCKOFFSET_BASEBIT
  - MEMORYCHUNK_VALUE_BASEBIT
- Called from (representative examples):
  - [AllocSetAllocChunkFromBlock](../A/AllocSetAllocChunkFromBlock.md) (aset.c:788)
  - [AllocSetAllocFromNewBlock](../A/AllocSetAllocFromNewBlock.md) (aset.c:872)
  - [BumpAllocChunkFromBlock](../B/BumpAllocChunkFromBlock.md) (bump.c:398)
  - [GenerationAllocChunkFromBlock](../G/GenerationAllocChunkFromBlock.md) (generation.c:431)
  - [MemoryContextAllocAligned](MemoryContextAllocAligned.md) (mcxt.c:1472)
  - [SlabAllocSetupNewChunk](../S/SlabAllocSetupNewChunk.md) (slab.c:514)

## Notes and Other Information
- This is an inline function for performance efficiency since it's called frequently during memory allocation
- The function includes several Assert statements to validate input parameters in debug builds
- Both chunk and block pointers must be MAXALIGN'd (aligned to maximum alignment boundary)
- The block offset calculation assumes chunk comes after block in memory
- This function is fundamental to PostgreSQL's memory chunk tracking system, enabling efficient memory management across different allocation strategies
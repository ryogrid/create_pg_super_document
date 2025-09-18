# GenerationAllocChunkFromBlock

## Location
src/backend/utils/mmgr/generation.c: 413 - 460

## Overview
A helper function that allocates a new chunk from an existing GenerationBlock, handling the common chunk initialization code shared between different allocation paths.

## Definition


## Detailed Description
GenerationAllocChunkFromBlock is an inline helper function that performs the actual chunk allocation within a GenerationBlock. It updates the block's metadata (chunk count and free pointer), initializes the chunk header, and applies various debugging and memory safety features. The function is designed to avoid code duplication between GenerationAlloc() and GenerationAllocFromNewBlock().

The function positions the new chunk at the current freeptr location, advances the freeptr by the chunk size plus header size, and sets up the chunk header with appropriate metadata. It includes comprehensive debugging support with sentinel bytes, memory randomization, and Valgrind annotations.

## Parameters / Member Variables
- : The Generation memory context (used for consistency, though not directly used in this function)
- : The GenerationBlock to allocate the chunk from (must have sufficient free space)
- : The requested allocation size in bytes
- : The actual chunk size after alignment (may be larger than size)

## Dependencies
- Functions called/Symbols referenced:
  - VALGRIND_MAKE_MEM_UNDEFINED
  - MemoryChunkSetHdrMask
  - set_sentinel (when MEMORY_CONTEXT_CHECKING)
  - randomize_mem (when RANDOMIZE_ALLOCATED_MEMORY)
  - MemoryChunkGetPointer
  - VALGRIND_MAKE_MEM_NOACCESS
  - pg_noinline
- Called from (representative examples):
  - GenerationAllocFromNewBlock
  - GenerationAlloc (multiple call sites)

## Notes and Other Information
- The function is marked as static inline for performance, being called frequently during allocation
- Includes assertions to validate the block has sufficient free space before allocation
- Updates block metadata: increments nchunks and advances freeptr
- Chunk header initialization uses MemoryChunkSetHdrMask with MCTX_GENERATION_ID
- Comprehensive memory debugging features including sentinel bytes and Valgrind annotations
- The function assumes the caller has already validated the allocation parameters
- Padding bytes between the requested size and chunk_size are marked as NOACCESS for error detection
- The chunk header itself is marked as NOACCESS after initialization to prevent accidental modification
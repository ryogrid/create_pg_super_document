# GenerationBlockFreeBytes

## Location
src/backend/utils/mmgr/generation.c: 654 - 663

## Overview
A simple static inline function that calculates and returns the number of free bytes remaining in a GenerationBlock for allocation purposes.

## Definition
```c
static inline Size
GenerationBlockFreeBytes(GenerationBlock *block)
```

## Detailed Description
GenerationBlockFreeBytes provides a fast calculation of available space within a memory block by computing the difference between the end pointer and the current free pointer. This function is essential for allocation decisions in the generation memory context, allowing the allocator to quickly determine whether a block has sufficient space for a requested allocation without complex iteration or tracking.

The function performs simple pointer arithmetic to determine contiguous free space. Since generation blocks allocate memory sequentially from the freeptr toward the endptr, the difference represents exactly how many bytes are available for new allocations. This approach is both efficient and reliable for the generation context's sequential allocation strategy.

## Parameters / Member Variables
- `block`: Pointer to the GenerationBlock to check for available space

## Dependencies
- Functions called/Symbols referenced:
  - GenerationBlock (operates on the block structure directly)
- Called from (representative examples):
  - IsKeeperBlock (checking keeper block space availability)
  - GenerationAlloc (determining if current block has sufficient space)
  - GenerationAlloc (checking freeblock space for reuse decisions)

## Notes and Other Information
- Marked as static inline for maximum performance optimization
- Simple pointer arithmetic operation with no overhead
- Critical for allocation path performance in generation contexts
- Returns Size type (typically size_t) for consistent memory size handling
- Used heavily in allocation decision logic throughout generation memory management
- Enables efficient space checking without walking allocated chunks
- Essential component of the generation context's block reuse strategy
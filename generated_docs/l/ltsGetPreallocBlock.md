# ltsGetPreallocBlock

## Location
[src/backend/utils/sort/logtape.c:431-468](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L431-L468)

## Overview
Manages per-tape block preallocation by maintaining a private cache of blocks in descending order to optimize sequential write performance.

## Definition
```c
static int64 ltsGetPreallocBlock(LogicalTapeSet *lts, LogicalTape *lt)
```

## Detailed Description
This function implements a sophisticated preallocation strategy for individual logical tapes. It maintains a per-tape cache of preallocated blocks sorted in descending order, allowing efficient allocation of the lowest available block numbers. The function operates in several phases:

1. **Direct allocation**: If preallocated blocks exist, returns the lowest one (stored at the end of the descending-ordered array)
2. **Cache initialization**: If no cache exists, allocates initial cache with `TAPE_WRITE_PREALLOC_MIN` size
3. **Cache expansion**: If cache is exhausted, doubles the size up to `TAPE_WRITE_PREALLOC_MAX`
4. **Cache refill**: Populates the entire cache by calling `ltsGetFreeBlock()` repeatedly

The blocks are stored in descending order to enable efficient popping from the end of the array, ensuring the lowest block numbers are allocated first.

## Parameters / Member Variables
- `lts`: Pointer to the LogicalTapeSet containing the global free block management
- `lt`: Pointer to the specific LogicalTape that needs a preallocated block

## Dependencies
- Functions called/Symbols referenced:
  - ltsGetFreeBlock
  - palloc
  - repalloc
  - TAPE_WRITE_PREALLOC_MIN
  - TAPE_WRITE_PREALLOC_MAX
  - LogicalTapeSet (struct)
  - LogicalTape (struct)
- Called from (representative examples):
  - ltsGetBlock

## Notes and Other Information
- Returns the lowest available preallocated block number to minimize fragmentation
- Uses exponential growth strategy (doubling) for cache size with a maximum limit
- Blocks are arranged in descending order for efficient stack-like access
- Includes assertion checking to verify descending order is maintained
- This optimization reduces contention on the global free list and improves locality
- The preallocation size grows from `TAPE_WRITE_PREALLOC_MIN` to `TAPE_WRITE_PREALLOC_MAX`
- Memory is managed using PostgreSQL's palloc/repalloc memory management functions
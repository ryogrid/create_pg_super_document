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
  - [ltsGetFreeBlock](ltsGetFreeBlock.md)
  - [palloc](../p/palloc.md)
  - [repalloc](../r/repalloc.md)
  - TAPE_WRITE_PREALLOC_MIN
  - TAPE_WRITE_PREALLOC_MAX
  - [LogicalTapeSet](../L/LogicalTapeSet.md) (struct)
  - [LogicalTape](../L/LogicalTape.md) (struct)
- Called from (representative examples):
  - [ltsGetBlock](ltsGetBlock.md)

## Notes and Other Information
- Returns the lowest available preallocated block number to minimize fragmentation
- Uses exponential growth strategy (doubling) for cache size with a maximum limit
- Blocks are arranged in descending order for efficient stack-like access
- Includes assertion checking to verify descending order is maintained
- This optimization reduces contention on the global free list and improves locality
- The preallocation size grows from `TAPE_WRITE_PREALLOC_MIN` to `TAPE_WRITE_PREALLOC_MAX`
- Memory is managed using PostgreSQL's palloc/repalloc memory management functions

## Simplified Source

```c
static int64 ltsGetPreallocBlock(LogicalTapeSet *lts, LogicalTape *lt) {
    // If we have preallocated blocks, return the lowest one
    // (stored at end of descending-ordered array)
    if (lt->nprealloc > 0) {
        return lt->prealloc[--lt->nprealloc];
    }

    // Initialize preallocation cache if needed
    if (lt->prealloc == NULL) {
        lt->prealloc_size = TAPE_WRITE_PREALLOC_MIN;
        lt->prealloc = palloc(sizeof(int64) * lt->prealloc_size);
    }
    // Expand cache if not at maximum size
    else if (lt->prealloc_size < TAPE_WRITE_PREALLOC_MAX) {
        lt->prealloc_size *= 2;  // Double the size
        if (lt->prealloc_size > TAPE_WRITE_PREALLOC_MAX) {
            lt->prealloc_size = TAPE_WRITE_PREALLOC_MAX;
        }
        lt->prealloc = repalloc(lt->prealloc, sizeof(int64) * lt->prealloc_size);
    }

    // Refill the entire preallocation cache
    lt->nprealloc = lt->prealloc_size;
    for (int i = lt->nprealloc; i > 0; i--) {
        // Get blocks from global free list in descending order
        lt->prealloc[i - 1] = ltsGetFreeBlock(lts);

        // Verify blocks are in descending order
        Assert(i == lt->nprealloc || lt->prealloc[i - 1] > lt->prealloc[i]);
    }

    // Return the lowest preallocated block
    return lt->prealloc[--lt->nprealloc];
}
```
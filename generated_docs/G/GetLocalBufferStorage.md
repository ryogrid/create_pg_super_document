# GetLocalBufferStorage

## Location
[src/backend/storage/buffer/localbuf.c:728-785](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/buffer/localbuf.c#L728-L785)

## Overview
GetLocalBufferStorage allocates memory for local buffers using an efficient block allocation strategy to minimize memory management overhead.

## Definition
```c
static Block GetLocalBufferStorage(void)
```

## Detailed Description
GetLocalBufferStorage implements an efficient memory allocation strategy for local buffers by aggregating allocation requests into larger blocks. Instead of requesting memory for each buffer individually, the function allocates multiple buffers at once and then distributes them from the current block as needed.

The function uses a doubling allocation strategy: it starts with 16 buffers per block and doubles the size for each subsequent allocation, subject to limits based on the total number of buffers needed and the maximum allocation size. This approach reduces memory fragmentation and overhead from the memory manager.

The function maintains several static variables to track the current allocation state, including the current memory block, the position within that block, and the total number of buffers allocated. All allocations are made in a dedicated memory context (LocalBufferContext) for easy identification in memory usage reports.

The allocated buffers are aligned to I/O boundaries using PG_IO_ALIGN_SIZE to ensure optimal performance for disk operations.

## Parameters / Member Variables
This function takes no parameters but maintains several static variables:
- `cur_block`: Pointer to the current memory block being used for allocation
- `next_buf_in_block`: Index of the next buffer to allocate within the current block
- `num_bufs_in_block`: Total number of buffers that can be allocated from the current block
- `total_bufs_allocated`: Total number of buffers allocated so far
- `LocalBufferContext`: Memory context used for all local buffer allocations

## Dependencies
- Functions called/Symbols referenced:
  - AllocSetContextCreate (for creating memory context)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (for memory allocation)
  - TYPEALIGN (for memory alignment)
- Constants referenced:
  - ALLOCSET_DEFAULT_SIZES
  - MaxAllocSize
  - PG_IO_ALIGN_SIZE
  - BLCKSZ (block size)
- Global variables accessed:
  - NLocBuffer (total number of local buffers configured)
  - TopMemoryContext (parent memory context)
- Called from (representative examples):
  - LocalBufHdrGetBlock
  - [GetLocalVictimBuffer](GetLocalVictimBuffer.md)

## Notes and Other Information
- The function is static, meaning it's only accessible within the localbuf.c file
- Memory allocation uses an exponential growth strategy to balance efficiency with memory usage
- All allocations are I/O aligned for optimal disk performance
- The function creates the LocalBufferContext on first use for better memory tracking
- Once allocated, local buffer memory is never freed during the process lifetime
- The allocation strategy minimizes the number of calls to the memory manager, reducing overhead
- The function includes bounds checking to prevent over-allocation beyond the configured limit

## Simplified Source

```c
static Block GetLocalBufferStorage(void) {
    static char *cur_block = NULL;
    static int next_buf_in_block = 0;
    static int num_bufs_in_block = 0;
    static int total_bufs_allocated = 0;
    static MemoryContext LocalBufferContext = NULL;

    char *this_buf;

    // Check if we need a new memory block
    if (next_buf_in_block >= num_bufs_in_block) {
        // Create memory context on first use
        if (LocalBufferContext == NULL) {
            LocalBufferContext = AllocSetContextCreate(TopMemoryContext,
                                                       "LocalBufferContext",
                                                       ALLOCSET_DEFAULT_SIZES);
        }

        // Calculate number of buffers to allocate (doubling strategy)
        int num_bufs = Max(num_bufs_in_block * 2, 16);
        num_bufs = Min(num_bufs, NLocBuffer - total_bufs_allocated);
        num_bufs = Min(num_bufs, MaxAllocSize / BLCKSZ);

        // Allocate I/O-aligned memory block
        cur_block = (char *) TYPEALIGN(PG_IO_ALIGN_SIZE,
                                       MemoryContextAlloc(LocalBufferContext,
                                                          num_bufs * BLCKSZ + PG_IO_ALIGN_SIZE));
        next_buf_in_block = 0;
        num_bufs_in_block = num_bufs;
    }

    // Return next buffer from current block
    this_buf = cur_block + next_buf_in_block * BLCKSZ;
    next_buf_in_block++;
    total_bufs_allocated++;

    return (Block) this_buf;
}
```
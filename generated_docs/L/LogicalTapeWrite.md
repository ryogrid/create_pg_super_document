# LogicalTapeWrite

## Location
[src/backend/utils/sort/logtape.c:761-845](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/logtape.c#L761-L845)

## Overview
Writes data to a logical tape, handling buffer management, block allocation, and chaining of tape blocks during external sorting operations.

## Definition
```c
void LogicalTapeWrite(LogicalTape *lt, const void *ptr, size_t size)
```

## Detailed Description
The `LogicalTapeWrite` function is the primary interface for writing data to a logical tape. It handles all aspects of buffered writing including lazy buffer allocation, block management, and linking blocks together in a chain structure. The function can handle writes of arbitrary size by automatically splitting large writes across multiple tape blocks.

On the first write to a tape, the function allocates both the I/O buffer (BLCKSZ bytes) and the first tape block. It initializes the block chain by setting up the trailer information with appropriate previous/next block pointers. For subsequent writes, it manages the buffer by writing data until the buffer is full, then allocating a new block, writing the current buffer to storage, and continuing with the new block.

The function maintains block linking by storing next block numbers in each block's trailer before writing it to storage. This creates a linked list of blocks that can be traversed during read operations. The function also tracks the dirty state of the buffer and various position counters to manage the write process efficiently.

## Parameters / Member Variables
- `lt`: Pointer to the LogicalTape to write to
- `ptr`: Pointer to the data to write
- `size`: Number of bytes to write

## Dependencies
- Functions called/Symbols referenced:
  - [ltsGetBlock](../l/ltsGetBlock.md) (allocates new tape blocks)
  - [ltsWriteBlock](../l/ltsWriteBlock.md) (writes blocks to storage)
  - TapeBlockGetTrailer (accesses block trailer)
  - TapeBlockPayloadSize (gets usable space in a block)
  - [palloc](../p/palloc.md) (for buffer allocation)
  - memcpy (for data copying)
  - [LogicalTape](LogicalTape.md) (structure type)
  - [LogicalTapeSet](LogicalTapeSet.md) (structure type)
- Called from (representative examples):
  - [hashagg_spill_tuple](../h/hashagg_spill_tuple.md)
  - [markrunend](../m/markrunend.md)
  - [writetup_heap](../w/writetup_heap.md)
  - [writetup_cluster](../w/writetup_cluster.md)
  - [writetup_index](../w/writetup_index.md)
  - [writetup_index_brin](../w/writetup_index_brin.md)
  - [writetup_datum](../w/writetup_datum.md)

## Notes and Other Information
- Uses lazy allocation for I/O buffer (allocated on first write)
- Automatically handles block chaining with prev/next pointers in block trailers
- Can handle writes larger than block size by spanning multiple blocks
- Maintains dirty flag to track buffer state
- Asserts that tape must be in writing mode and at offset 0
- No error returns - uses ereport() on failure
- Buffer size is fixed at BLCKSZ (typically 8KB)
- Efficiently handles partial buffer fills and automatic flushing when full
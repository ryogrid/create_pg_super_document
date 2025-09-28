# heap_fetch_next_buffer

## Location
[src/backend/access/heap/heapam.c:629-673](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam.c#L629-L673)

## Overview
heap_fetch_next_buffer is an internal function that advances a heap scan to the next buffer by reading and pinning the next block from the relation's main fork using PostgreSQL's read stream infrastructure.

## Definition

```c
static inline void
heap_fetch_next_buffer(HeapScanDesc scan, ScanDirection dir)
```
## Detailed Description
This function manages buffer transitions during heap scanning by coordinating with the read stream mechanism to fetch the next page. It handles proper cleanup of the previous buffer, manages scan direction changes, and provides interrupt checking to ensure responsive behavior during long sequential scans. The function is critical for implementing efficient sequential and backward scans by managing prefetching and buffer lifecycle. When the scan direction changes, it resets the prefetch mechanism to avoid incorrect block prefetching patterns.

## Parameters / Member Variables
- `scan`: HeapScanDesc containing scan state including current buffer, block number, and read stream
- `dir`: ScanDirection indicating forward or backward scan direction

## Dependencies
- Functions called/Symbols referenced:
  - [BufferIsValid](../B/BufferIsValid.md)
  - [ReleaseBuffer](../R/ReleaseBuffer.md)
  - CHECK_FOR_INTERRUPTS
  - [read_stream_reset](../r/read_stream_reset.md)
  - [read_stream_next_buffer](../r/read_stream_next_buffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
- Called from (representative examples):
  - [heapgettup](heapgettup.md)
  - [heapgettup_pagemode](heapgettup_pagemode.md)

## Notes and Other Information
- Marked as static inline for performance optimization
- Requires scan->rs_read_stream to be initialized
- Automatically releases previous buffer before acquiring new one
- Provides interrupt checking to handle long-running sequential scans
- Handles scan direction changes by resetting read stream prefetch state
- Updates both rs_cbuf (current buffer) and rs_cblock (current block number) in scan descriptor
- Essential component of PostgreSQL's streaming read infrastructure for heap access

## Simplified Source

```c
// Simplified version of heap_fetch_next_buffer
static inline void heap_fetch_next_buffer(HeapScanDesc scan, ScanDirection dir) {
    Assert(scan->rs_read_stream);

    // Release previous buffer if any
    if (BufferIsValid(scan->rs_cbuf)) {
        ReleaseBuffer(scan->rs_cbuf);
        scan->rs_cbuf = InvalidBuffer;
    }

    // Check for interrupts during long scans
    CHECK_FOR_INTERRUPTS();

    // Handle scan direction changes
    if (unlikely(scan->rs_dir != dir)) {
        scan->rs_prefetch_block = scan->rs_cblock;
        read_stream_reset(scan->rs_read_stream);
    }

    // Update scan direction and fetch next buffer
    scan->rs_dir = dir;
    scan->rs_cbuf = read_stream_next_buffer(scan->rs_read_stream, NULL);

    // Update current block number if buffer is valid
    if (BufferIsValid(scan->rs_cbuf))
        scan->rs_cblock = BufferGetBlockNumber(scan->rs_cbuf);
}
```

Key simplifications made:
- Core logic: release previous buffer, handle direction changes, fetch next buffer
- Direction change detection resets prefetch to avoid incorrect block ordering
- Interrupt checking ensures responsiveness during long sequential scans
- Read stream infrastructure manages actual buffer fetching and prefetching
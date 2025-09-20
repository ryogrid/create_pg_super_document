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
  - ReleaseBuffer
  - CHECK_FOR_INTERRUPTS
  - read_stream_reset
  - read_stream_next_buffer
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
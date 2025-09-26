# heapam_scan_analyze_next_block

## Location
[src/backend/access/heap/heapam_handler.c:1006-1029](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/heap/heapam_handler.c#L1006-L1029)

## Overview
This function advances to the next block in a heap relation during ANALYZE operations, managing buffer pins and locks for statistical sampling.

## Definition
static bool heapam_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)

## Detailed Description
heapam_scan_analyze_next_block is a specialized function used during ANALYZE operations to prepare the next heap block for statistical analysis. It reads the next buffer from the provided read stream and sets up the necessary buffer management (pinning and locking) to ensure data consistency during tuple analysis. The function maintains a shared lock on the buffer throughout the analysis of all tuples in the block to prevent concurrent modifications such as HOT pruning from interfering with the sampling process. This function must be called on scans that were started with the SO_TYPE_ANALYZE option.

## Parameters / Member Variables
- `scan`: TableScanDesc representing the heap scan descriptor for the ANALYZE operation
- `stream`: ReadStream pointer for sequential buffer access during the analysis

## Dependencies
- Functions called/Symbols referenced:
  - [read_stream_next_buffer](../r/read_stream_next_buffer.md)
  - [BufferIsValid](../B/BufferIsValid.md)
  - [LockBuffer](../L/LockBuffer.md)
  - [BufferGetBlockNumber](../B/BufferGetBlockNumber.md)
- Constants referenced:
  - BUFFER_LOCK_SHARE
  - FirstOffsetNumber
- Called from (representative examples):
  - [SampleHeapTupleVisible](../S/SampleHeapTupleVisible.md) (referenced in heapam_handler.c:2633)

## Notes and Other Information
- This is a static function, only accessible within heapam_handler.c
- Returns true if a valid block was obtained, false if the stream is exhausted
- Maintains both a buffer pin and shared lock until all tuples in the block are processed
- The buffer pin comes from the read stream already established
- Sets up rs_cbuf (current buffer), rs_cblock (current block number), and rs_cindex (starting at FirstOffsetNumber)
- Designed to work in conjunction with heapam_scan_analyze_next_tuple() for complete block analysis
- The shared lock is held throughout the entire block analysis to avoid lock traffic overhead for individual tuples
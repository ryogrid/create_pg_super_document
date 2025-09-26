# read_stream_next_buffer

## Location
src/backend/storage/aio/read_stream.c: 567 - 783

## Overview
Retrieves the next pinned buffer from a read stream, managing I/O completion, buffer advancement, and lookahead operations for optimal sequential access.

## Definition


## Detailed Description
This function implements the core buffer retrieval mechanism for read streams, featuring a sophisticated fast path optimization for all-cached scans and a full path for handling I/O operations. The function manages buffer queues, waits for pending I/O operations to complete, and adjusts the lookahead distance based on cache hit/miss patterns.

The fast path is optimized for scenarios where all data is already cached (behavior A), avoiding queue management overhead and using simple singular buffer reads. The full path handles complex scenarios involving pending I/O operations, dynamic distance adjustment, and per-buffer data management.

## Parameters / Member Variables
- : The read stream object containing buffer queues and I/O state
- : Optional output parameter for per-buffer callback data (valid until next call)

## Dependencies
- Functions called/Symbols referenced:
  - read_stream_get_block
  - StartReadBuffer
  - WaitReadBuffers
  - read_stream_look_ahead
  - get_per_buffer_data
  - wipe_mem (debug builds)
- Called from (representative examples):
  - heap_fetch_next_buffer
  - heapam_scan_analyze_next_block
  - read_stream_reset

## Notes and Other Information
- Features a fast path optimization for all-cached sequential scans that bypasses queue management
- Implements adaptive distance adjustment: exponential growth for cache hits (behavior C) and controlled growth toward io_combine_limit for cache misses (behavior B)
- Includes comprehensive debugging support with memory clobbering and Valgrind integration
- Returns InvalidBuffer when the stream is exhausted or no more blocks are available
- Manages buffer pin transfers from stream to caller, maintaining accurate pin counts
- The per_buffer_data pointer remains valid only until the next call to this function
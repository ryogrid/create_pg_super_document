# read_stream_reset

## Location
[src/backend/storage/aio/read_stream.c:797-822](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L797-L822)

## Overview
Resets a read stream by releasing all queued buffers and reinitializing the stream state, allowing it to be reused for reading different blocks.

## Definition

```c
void
read_stream_reset(ReadStream *stream)
```
## Detailed Description
This function provides a mechanism to completely reset a read stream's internal state without destroying and recreating the stream object. It releases all currently pinned buffers, cancels any pending I/O operations, and resets the lookahead algorithm to its initial state.

The function is particularly useful for scenarios where a scan needs to be restarted from a different position, or when speculative reads need to be discarded. After reset, the stream can be reused as if it were newly created, starting with conservative assumptions about data locality.

## Parameters / Member Variables
- : The read stream object to reset

## Dependencies
- Functions called/Symbols referenced:
  - read_stream_next_buffer
  - ReleaseBuffer
- Called from (representative examples):
  - heap_fetch_next_buffer
  - heap_rescan
  - read_stream_end

## Notes and Other Information
- Releases all pinned buffers that haven't been consumed by the caller
- Resets distance to 1, returning to conservative single-block lookahead
- Clears the fast path optimization state and buffered block number
- Ensures no I/O operations remain in progress after reset
- Safe to call multiple times on the same stream
- After reset, the stream assumes data is likely cached and starts with minimal lookahead
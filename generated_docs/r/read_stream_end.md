# read_stream_end

## Location
src/backend/storage/aio/read_stream.c: 823 - 827

## Overview
Properly releases and deallocates a read stream object, cleaning up all associated resources.

## Definition

```c
void
read_stream_end(ReadStream *stream)
```
## Detailed Description
This function provides the proper cleanup mechanism for read stream objects. It first calls read_stream_reset to ensure all pinned buffers are released and any pending I/O operations are completed, then deallocates the memory associated with the stream object.

The function is the counterpart to read_stream_begin_relation and must be called to prevent memory leaks and ensure proper resource cleanup. It handles all aspects of stream cleanup including buffers, I/O tracking structures, and per-buffer data.

## Parameters / Member Variables
- : The read stream object to release and deallocate

## Dependencies
- Functions called/Symbols referenced:
  - read_stream_reset
  - pfree
- Called from (representative examples):
  - heap_endscan
  - acquire_sample_rows

## Notes and Other Information
- Must be called for every read stream created with read_stream_begin_relation
- Automatically handles cleanup of all pinned buffers and pending I/O operations via read_stream_reset
- Safe to call even if the stream has already been reset or partially consumed
- After calling this function, the stream pointer becomes invalid and must not be used
- Deallocates the single memory allocation made in read_stream_begin_relation containing the stream object, buffer array, I/O tracking, and per-buffer data
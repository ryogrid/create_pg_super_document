# read_stream_next_buffer

## Location
[src/backend/storage/aio/read_stream.c:567-783](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L567-L783)

## Overview
Retrieves the next pinned buffer from a read stream, managing I/O completion, buffer advancement, and lookahead operations for optimal sequential access.

## Definition

```c
Buffer
read_stream_next_buffer(ReadStream *stream, void **per_buffer_data)
```
## Detailed Description
This function implements the core buffer retrieval mechanism for read streams, featuring a sophisticated fast path optimization for all-cached scans and a full path for handling I/O operations. The function manages buffer queues, waits for pending I/O operations to complete, and adjusts the lookahead distance based on cache hit/miss patterns.

The fast path is optimized for scenarios where all data is already cached (behavior A), avoiding queue management overhead and using simple singular buffer reads. The full path handles complex scenarios involving pending I/O operations, dynamic distance adjustment, and per-buffer data management.

## Parameters / Member Variables
- `*stream`: The read stream object containing buffer queues and I/O state
- `**per_buffer_data`: Optional output parameter for per-buffer callback data (valid until next call)
## Dependencies
- Functions called/Symbols referenced:
  - [read_stream_get_block](read_stream_get_block.md)
  - [StartReadBuffer](../S/StartReadBuffer.md)
  - [WaitReadBuffers](../W/WaitReadBuffers.md)
  - [read_stream_look_ahead](read_stream_look_ahead.md)
  - [get_per_buffer_data](../g/get_per_buffer_data.md)
  - [wipe_mem](../w/wipe_mem.md) (debug builds)
- Called from (representative examples):
  - [heap_fetch_next_buffer](../h/heap_fetch_next_buffer.md)
  - [heapam_scan_analyze_next_block](../h/heapam_scan_analyze_next_block.md)
  - [read_stream_reset](read_stream_reset.md)

## Notes and Other Information
- Features a fast path optimization for all-cached sequential scans that bypasses queue management
- Implements adaptive distance adjustment: exponential growth for cache hits (behavior C) and controlled growth toward io_combine_limit for cache misses (behavior B)
- Includes comprehensive debugging support with memory clobbering and Valgrind integration
- Returns InvalidBuffer when the stream is exhausted or no more blocks are available
- Manages buffer pin transfers from stream to caller, maintaining accurate pin counts
- The per_buffer_data pointer remains valid only until the next call to this function

## Simplified Source

```c
Buffer
read_stream_next_buffer(ReadStream *stream, void **per_buffer_data)
{
    Buffer buffer;
    int16 oldest_buffer_index;

    // Fast path for all-cached scans (no I/O needed)
    if (likely(stream->fast_path)) {
        BlockNumber next_blocknum;

        // Return buffer from previous call
        oldest_buffer_index = stream->oldest_buffer_index;
        buffer = stream->buffers[oldest_buffer_index];

        // Get next block to pin
        next_blocknum = read_stream_get_block(stream, NULL);

        if (likely(next_blocknum != InvalidBlockNumber)) {
            // Pin buffer for next call
            if (likely(!StartReadBuffer(&stream->ios[0].op,
                                      &stream->buffers[oldest_buffer_index],
                                      next_blocknum,
                                      stream->advice_enabled ?
                                      READ_BUFFERS_ISSUE_ADVICE : 0))) {
                return buffer;  // Fast return - no I/O needed
            }

            // Next call must wait for I/O
            stream->oldest_io_index = 0;
            stream->next_io_index = stream->max_ios > 1 ? 1 : 0;
            stream->ios_in_progress = 1;
            stream->ios[0].buffer_index = oldest_buffer_index;
            stream->seq_blocknum = next_blocknum + 1;
        } else {
            // End of stream
            stream->distance = 0;
            stream->oldest_buffer_index = stream->next_buffer_index;
            stream->pinned_buffers = 0;
        }

        stream->fast_path = false;
        return buffer;
    }

    // Check if stream is empty
    if (unlikely(stream->pinned_buffers == 0)) {
        if (stream->distance == 0)
            return InvalidBuffer;

        // Prime the stream
        read_stream_look_ahead(stream, true);
        if (stream->pinned_buffers == 0)
            return InvalidBuffer;
    }

    // Get oldest buffer and per-buffer data
    oldest_buffer_index = stream->oldest_buffer_index;
    buffer = stream->buffers[oldest_buffer_index];
    if (per_buffer_data)
        *per_buffer_data = get_per_buffer_data(stream, oldest_buffer_index);

    // Wait for I/O if needed
    if (stream->ios_in_progress > 0 &&
        stream->ios[stream->oldest_io_index].buffer_index == oldest_buffer_index) {
        int16 io_index = stream->oldest_io_index;
        int16 distance;

        WaitReadBuffers(&stream->ios[io_index].op);

        stream->ios_in_progress--;
        if (++stream->oldest_io_index == stream->max_ios)
            stream->oldest_io_index = 0;

        // Adjust distance based on I/O behavior
        if (stream->ios[io_index].op.flags & READ_BUFFERS_ISSUE_ADVICE) {
            // Fast ramp up for cache hits
            distance = stream->distance * 2;
            distance = Min(distance, stream->max_pinned_buffers);
            stream->distance = distance;
        } else {
            // Controlled growth for cache misses
            if (stream->distance > stream->io_combine_limit) {
                stream->distance--;
            } else {
                distance = stream->distance * 2;
                distance = Min(distance, stream->io_combine_limit);
                distance = Min(distance, stream->max_pinned_buffers);
                stream->distance = distance;
            }
        }
    }

    // Transfer pin to caller and advance
    stream->pinned_buffers--;
    stream->oldest_buffer_index++;
    if (stream->oldest_buffer_index == stream->queue_size)
        stream->oldest_buffer_index = 0;

    // Prepare for next call
    read_stream_look_ahead(stream, false);

    // Check if we can use fast path next time
    if (stream->ios_in_progress == 0 &&
        stream->pinned_buffers == 1 &&
        stream->distance == 1 &&
        stream->pending_read_nblocks == 0 &&
        stream->per_buffer_data_size == 0) {
        stream->fast_path = true;
    }

    return buffer;
}
```
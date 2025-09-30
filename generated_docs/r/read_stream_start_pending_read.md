# read_stream_start_pending_read

## Location
[src/backend/storage/aio/read_stream.c:212-301](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L212-L301)

## Overview
Initiates an asynchronous read operation for pending blocks in a ReadStream, managing buffer allocation and I/O coordination.

## Definition
```c
static void read_stream_start_pending_read(ReadStream *stream, bool suppress_advice)
```

## Detailed Description
This function is responsible for starting asynchronous read operations for blocks that have been queued for reading. It coordinates with the buffer management system to allocate buffers, tracks I/O operations in progress, and handles buffer wraparound in the circular queue. The function also implements read-ahead distance adjustments and memory advice mechanisms to optimize I/O performance. It handles partial reads by adjusting the pending read state and manages the circular buffer index calculations to ensure proper buffer reuse.

## Parameters / Member Variables
- `stream`: Pointer to the ReadStream structure containing all state and configuration
- `suppress_advice`: Boolean flag to disable memory advice hints for this read operation

## Dependencies
- Functions called/Symbols referenced:
  - [ReadStream](../R/ReadStream.md) (structure type)
  - READ_BUFFERS_ISSUE_ADVICE (flag constant)
  - [StartReadBuffers](../S/StartReadBuffers.md) (I/O initiation function)
  - Assert (debugging macro)
  - memmove (memory move function)
- Called from (representative examples):
  - [read_stream_look_ahead](read_stream_look_ahead.md) (multiple call sites)

## Notes and Other Information
- The function includes extensive assertions to validate stream state before and during operation
- Implements look-ahead distance decay when I/O is not needed (cached data)
- Handles circular buffer wraparound by sliding overflow buffers to the front of the array
- Manages the relationship between pending reads, pinned buffers, and I/O operations in progress
- The suppress_advice parameter allows callers to disable memory advice for specific scenarios
- Updates sequential block tracking for advice generation in subsequent reads
- Handles partial reads by adjusting pending_read_blocknum and pending_read_nblocks accordingly

## Simplified Source

```c
static void
read_stream_start_pending_read(ReadStream *stream, bool suppress_advice)
{
    bool need_wait;
    int nblocks;
    int flags;
    int16 io_index;
    int16 buffer_index;

    // Validate stream state
    Assert(stream->pending_read_nblocks > 0);
    Assert(stream->pinned_buffers + stream->pending_read_nblocks <=
           stream->max_pinned_buffers);

    // Determine if we should issue memory advice
    if (!suppress_advice &&
        stream->advice_enabled &&
        stream->pending_read_blocknum != stream->seq_blocknum)
        flags = READ_BUFFERS_ISSUE_ADVICE;
    else
        flags = 0;

    // Start the actual I/O operation
    buffer_index = stream->next_buffer_index;
    io_index = stream->next_io_index;
    nblocks = stream->pending_read_nblocks;

    need_wait = StartReadBuffers(&stream->ios[io_index].op,
                                &stream->buffers[buffer_index],
                                stream->pending_read_blocknum,
                                &nblocks,
                                flags);

    stream->pinned_buffers += nblocks;

    // Handle synchronous vs asynchronous I/O completion
    if (!need_wait) {
        // Data was already cached, reduce look-ahead distance
        if (stream->distance > 1)
            stream->distance--;
    } else {
        // Track async I/O operation for later completion
        stream->ios[io_index].buffer_index = buffer_index;
        if (++stream->next_io_index == stream->max_ios)
            stream->next_io_index = 0;
        stream->ios_in_progress++;
        stream->seq_blocknum = stream->pending_read_blocknum + nblocks;
    }

    // Handle buffer wraparound in circular queue
    int16 overflow = (buffer_index + nblocks) - stream->queue_size;
    if (overflow > 0) {
        memmove(&stream->buffers[0],
                &stream->buffers[stream->queue_size],
                sizeof(stream->buffers[0]) * overflow);
    }

    // Update buffer position for next read
    buffer_index += nblocks;
    if (buffer_index >= stream->queue_size)
        buffer_index -= stream->queue_size;
    stream->next_buffer_index = buffer_index;

    // Adjust pending read for remaining blocks
    stream->pending_read_blocknum += nblocks;
    stream->pending_read_nblocks -= nblocks;
}
```
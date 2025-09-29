# read_stream_look_ahead

## Location
[src/backend/storage/aio/read_stream.c:302-388](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L302-L388)

## Overview
Implements read-ahead logic by fetching block numbers from the callback and building optimal read requests while respecting I/O limits and distance constraints.

## Definition
```c
static void read_stream_look_ahead(ReadStream *stream, bool suppress_advice)
```

## Detailed Description
This function is the core of the ReadStream's read-ahead mechanism. It repeatedly calls the callback to determine which blocks should be read next, attempts to merge consecutive blocks into larger I/O operations, and manages the balance between read-ahead distance and I/O resource limits. The function builds pending reads by combining consecutive blocks up to the io_combine_limit, and starts I/O operations when optimal conditions are met. It handles buffer allocation within the circular queue, manages wraparound calculations without expensive modulo operations, and implements sophisticated logic to determine when to start pending reads versus continuing to build larger requests.

## Parameters / Member Variables
- `stream`: Pointer to the ReadStream structure containing all state and configuration
- `suppress_advice`: Boolean flag to disable memory advice hints for the first read operation

## Dependencies
- Functions called/Symbols referenced:
  - [ReadStream](../R/ReadStream.md) (structure type)
  - [read_stream_start_pending_read](read_stream_start_pending_read.md) (function to initiate I/O operations)
  - [get_per_buffer_data](../g/get_per_buffer_data.md) (function to get per-buffer data pointer)
  - [read_stream_get_block](read_stream_get_block.md) (function to get next block number from callback)
  - [read_stream_unget_block](read_stream_unget_block.md) (function to defer block processing)
  - InvalidBlockNumber (constant for end-of-stream sentinel)
- Called from (representative examples):
  - [read_stream_next_buffer](read_stream_next_buffer.md) (multiple call sites)

## Notes and Other Information
- The function implements sophisticated heuristics for when to start pending reads versus continuing to build larger requests
- Uses efficient circular buffer index calculation avoiding expensive modulo operations
- Handles end-of-stream conditions by setting distance to 0
- Implements read merging for consecutive blocks to optimize I/O performance
- Manages the balance between read-ahead distance and available I/O slots
- The suppress_advice parameter is only applied to the first read operation in the sequence
- Includes logic to unget blocks when I/O limits are reached mid-operation
- Prioritizes building full io_combine_limit sized reads when possible for maximum I/O efficiency

## Simplified Source

```c
static void
read_stream_look_ahead(ReadStream *stream, bool suppress_advice)
{
    // Main loop: build read requests while respecting limits
    while (stream->ios_in_progress < stream->max_ios &&
           stream->pinned_buffers + stream->pending_read_nblocks < stream->distance)
    {
        BlockNumber blocknum;
        int16 buffer_index;
        void *per_buffer_data;

        // Start pending read if we've hit combine limit
        if (stream->pending_read_nblocks == stream->io_combine_limit) {
            read_stream_start_pending_read(stream, suppress_advice);
            suppress_advice = false;
            continue;
        }

        // Calculate buffer index with wrap-around
        buffer_index = stream->next_buffer_index + stream->pending_read_nblocks;
        if (buffer_index >= stream->queue_size)
            buffer_index -= stream->queue_size;

        // Get next block from callback
        per_buffer_data = get_per_buffer_data(stream, buffer_index);
        blocknum = read_stream_get_block(stream, per_buffer_data);

        if (blocknum == InvalidBlockNumber) {
            // End of stream
            stream->distance = 0;
            break;
        }

        // Try to merge with pending read if consecutive
        if (stream->pending_read_nblocks > 0 &&
            stream->pending_read_blocknum + stream->pending_read_nblocks == blocknum) {
            stream->pending_read_nblocks++;
            continue;
        }

        // Start existing pending read before building new one
        while (stream->pending_read_nblocks > 0) {
            read_stream_start_pending_read(stream, suppress_advice);
            suppress_advice = false;
            if (stream->ios_in_progress == stream->max_ios) {
                // Hit I/O limit - defer this block
                read_stream_unget_block(stream, blocknum);
                return;
            }
        }

        // Start new pending read
        stream->pending_read_blocknum = blocknum;
        stream->pending_read_nblocks = 1;
    }

    // Start pending read if conditions are met
    if (stream->pending_read_nblocks > 0 &&
        (stream->pending_read_nblocks == stream->io_combine_limit ||
         (stream->pending_read_nblocks == stream->distance &&
          stream->pinned_buffers == 0) ||
         stream->distance == 0) &&
        stream->ios_in_progress < stream->max_ios) {
        read_stream_start_pending_read(stream, suppress_advice);
    }
}
```
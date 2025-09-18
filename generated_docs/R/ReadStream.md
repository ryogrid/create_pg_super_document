# ReadStream

## Location
[src/backend/storage/aio/read_stream.c:109-160](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L109-L160)

## Overview
ReadStream is the core structure that manages asynchronous read-ahead operations for relation data access in PostgreSQL, providing efficient buffered I/O with look-ahead capabilities.

## Definition
```c
struct ReadStream
{
    int16       max_ios;
    int16       io_combine_limit;
    int16       ios_in_progress;
    int16       queue_size;
    int16       max_pinned_buffers;
    int16       pinned_buffers;
    int16       distance;
    bool        advice_enabled;

    /* One-block buffer to support 'ungetting' a block number */
    BlockNumber buffered_blocknum;

    /* The callback and opaque pointer for block number generation */
    ReadStreamBlockNumberCB callback;
    void       *callback_private_data;

    /* Next expected block, for detecting sequential access */
    BlockNumber seq_blocknum;

    /* The read operation we are currently preparing */
    BlockNumber pending_read_blocknum;
    int16       pending_read_nblocks;

    /* Space for buffers and optional per-buffer private data */
    size_t      per_buffer_data_size;
    void       *per_buffer_data;

    /* Read operations that have been started but not waited for yet */
    InProgressIO *ios;
    int16       oldest_io_index;
    int16       next_io_index;

    bool        fast_path;

    /* Circular queue of buffers */
    int16       oldest_buffer_index;    /* Next pinned buffer to return */
    int16       next_buffer_index;      /* Index of next buffer to pin */
    Buffer      buffers[FLEXIBLE_ARRAY_MEMBER];
};
```

## Detailed Description
ReadStream implements an advanced asynchronous I/O system designed to optimize sequential and near-sequential access patterns to relation data. It provides intelligent read-ahead capabilities that can significantly improve performance by overlapping computation with I/O operations.

The system maintains a circular queue of buffers and tracks multiple in-flight I/O operations concurrently. It uses a callback mechanism to determine which blocks to read next, allowing for flexible access patterns while maintaining optimal I/O efficiency. The stream automatically detects sequential access patterns and adjusts its behavior accordingly, including the ability to disable prefetch advice when sequential access is detected (since operating systems like Linux can handle this more efficiently).

The structure supports both maintenance operations (governed by maintenance_io_concurrency) and regular operations (governed by effective_io_concurrency), allowing proper resource management across different types of workloads.

## Parameters / Member Variables
- `max_ios`: Maximum number of concurrent I/O operations allowed
- `io_combine_limit`: Limit for combining multiple I/O operations into larger requests
- `ios_in_progress`: Current number of I/O operations in flight
- `queue_size`: Size of the buffer queue
- `max_pinned_buffers`: Maximum number of buffers that can be pinned simultaneously
- `pinned_buffers`: Current number of pinned buffers
- `distance`: Read-ahead distance for determining how far to look ahead
- `advice_enabled`: Whether prefetch advice to the OS is enabled
- `buffered_blocknum`: Single-block buffer for supporting 'unget' operations
- `callback`: Function pointer to callback that determines next block numbers to read
- `callback_private_data`: Opaque data passed to the callback function
- `seq_blocknum`: Next expected block number for sequential access detection
- `pending_read_blocknum`: Block number of the read operation currently being prepared
- `pending_read_nblocks`: Number of blocks in the pending read operation
- `per_buffer_data_size`: Size of optional per-buffer private data
- `per_buffer_data`: Pointer to per-buffer private data storage
- `ios`: Array of InProgressIO structures tracking ongoing operations
- `oldest_io_index`: Index of the oldest I/O operation in the queue
- `next_io_index`: Index where the next I/O operation will be placed
- `fast_path`: Boolean indicating if fast path optimizations are enabled
- `oldest_buffer_index`: Index of the next pinned buffer to return to caller
- `next_buffer_index`: Index of the next buffer to pin for incoming data
- `buffers`: Flexible array member containing the actual buffer queue

## Dependencies
- Functions called/Symbols referenced:
  - [InProgressIO](../I/InProgressIO.md)
  - ReadStreamBlockNumberCB (callback function type)
  - FLEXIBLE_ARRAY_MEMBER
- Called from (representative examples):
  - [heap_scan_stream_read_next_parallel](../h/heap_scan_stream_read_next_parallel.md)
  - [heap_scan_stream_read_next_serial](../h/heap_scan_stream_read_next_serial.md)
  - [heapam_scan_analyze_next_block](../h/heapam_scan_analyze_next_block.md)
  - [block_sampling_read_stream_next](../b/block_sampling_read_stream_next.md)
  - read_stream_begin_relation
  - read_stream_next_buffer
  - read_stream_next_block

## Notes and Other Information
- Part of PostgreSQL's asynchronous I/O infrastructure introduced for improved read-ahead performance
- Supports different operational modes via flags: READ_STREAM_DEFAULT, READ_STREAM_MAINTENANCE, READ_STREAM_SEQUENTIAL, READ_STREAM_FULL
- The circular buffer design provides efficient memory usage and prevents memory allocation during normal operation
- Integrates with PostgreSQL's buffer manager and can work with custom BufferAccessStrategy implementations
- The callback mechanism allows for sophisticated access patterns while maintaining the benefits of read-ahead
- Located in src/backend/storage/aio/read_stream.c:109-160
- Used extensively throughout the storage layer for table scans, index scans, and maintenance operations
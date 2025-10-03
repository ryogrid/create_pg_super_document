# read_stream_begin_relation

## Location
[src/backend/storage/aio/read_stream.c:389-566](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/aio/read_stream.c#L389-L566)

## Overview
Creates a new read stream object for efficiently reading blocks from a specific fork of a PostgreSQL relation using vectored I/O and prefetching strategies.

## Definition

```c
ReadStream *
read_stream_begin_relation(int flags,
						   BufferAccessStrategy strategy,
						   Relation rel,
						   ForkNumber forknum,
						   ReadStreamBlockNumberCB callback,
						   void *callback_private_data,
						   size_t per_buffer_data_size)
```
## Detailed Description
This function initializes a read stream that optimizes sequential and random access patterns by performing lookahead and combining multiple block reads into larger vectored I/O operations. The read stream manages a queue of pinned buffers and uses callbacks to determine which blocks to read next. It automatically adjusts the number of concurrent I/Os based on tablespace configuration and system capabilities.

The function calculates optimal buffer queue sizes, I/O concurrency limits, and prefetch behavior based on the relation type, access flags, and system configuration. It supports both catalog relations (with conservative settings) and user relations (with configurable tablespace-specific settings).

## Parameters / Member Variables
- `flags`: Control flags including READ_STREAM_MAINTENANCE, READ_STREAM_SEQUENTIAL, and READ_STREAM_FULL
- `strategy`: Buffer access strategy to control buffer replacement policy and pin limits
- `rel`: The relation to read from
- `forknum`: Fork number (main, FSM, VM, etc.) of the relation to read
- `callback`: Function to call for determining the next block number to read
- `*callback_private_data`: Private data passed to the callback function
- `per_buffer_data_size`: Size of additional data to allocate per buffer for callback use
## Dependencies
- Functions called/Symbols referenced:
  - [RelationGetSmgr](../R/RelationGetSmgr.md)
  - [IsCatalogRelation](../I/IsCatalogRelation.md)
  - [IsCatalogRelationOid](../I/IsCatalogRelationOid.md)
  - [get_tablespace_maintenance_io_concurrency](../g/get_tablespace_maintenance_io_concurrency.md)
  - [get_tablespace_io_concurrency](../g/get_tablespace_io_concurrency.md)
  - [GetAccessStrategyPinLimit](../G/GetAccessStrategyPinLimit.md)
  - SmgrIsTemp
  - [LimitAdditionalLocalPins](../L/LimitAdditionalLocalPins.md)
  - [LimitAdditionalPins](../L/LimitAdditionalPins.md)
- Called from (representative examples):
  - [heap_beginscan](../h/heap_beginscan.md)
  - [acquire_sample_rows](../a/acquire_sample_rows.md)

## Notes and Other Information
- Automatically detects and handles catalog relations with conservative I/O settings to avoid circular dependencies
- Supports prefetch advice on systems with USE_PREFETCH enabled, except when direct I/O is active
- Allocates all required memory (buffers, I/O tracking, per-buffer data) in a single allocation for efficiency
- Queue size includes overflow space to handle multi-block I/Os that might extend beyond the regular queue boundary
- The distance parameter starts at 1 for gradual ramp-up or at the combine limit for full relation scans

## Simplified Source

```c
ReadStream *
read_stream_begin_relation(int flags,
                           BufferAccessStrategy strategy,
                           Relation rel,
                           ForkNumber forknum,
                           ReadStreamBlockNumberCB callback,
                           void *callback_private_data,
                           size_t per_buffer_data_size)
{
    ReadStream *stream;
    SMgrRelation smgr = RelationGetSmgr(rel);

    // Determine max concurrent I/Os based on relation type and tablespace
    int max_ios;
    if (IsCatalogRelation(rel))
        max_ios = effective_io_concurrency;  // Conservative for catalog tables
    else if (flags & READ_STREAM_MAINTENANCE)
        max_ios = get_tablespace_maintenance_io_concurrency(smgr->smgr_rlocator.locator.spcOid);
    else
        max_ios = get_tablespace_io_concurrency(smgr->smgr_rlocator.locator.spcOid);

    // Calculate buffer queue sizes with overflow space
    int queue_overflow = io_combine_limit - 1;
    uint32 max_pinned_buffers = Max(max_ios * 4, io_combine_limit);

    // Apply strategy and system limits
    max_pinned_buffers = Min(GetAccessStrategyPinLimit(strategy), max_pinned_buffers);
    if (SmgrIsTemp(smgr))
        LimitAdditionalLocalPins(&max_pinned_buffers);
    else
        LimitAdditionalPins(&max_pinned_buffers);

    int16 queue_size = max_pinned_buffers + 1;

    // Allocate stream with buffers, I/O tracking, and per-buffer data
    size_t size = offsetof(ReadStream, buffers) +
                  sizeof(Buffer) * (queue_size + queue_overflow) +
                  sizeof(InProgressIO) * Max(1, max_ios) +
                  per_buffer_data_size * queue_size +
                  MAXIMUM_ALIGNOF * 2;

    stream = (ReadStream *) palloc(size);
    memset(stream, 0, offsetof(ReadStream, buffers));

    // Set up memory layout
    stream->ios = (InProgressIO *) MAXALIGN(&stream->buffers[queue_size + queue_overflow]);
    if (per_buffer_data_size > 0)
        stream->per_buffer_data = (void *) MAXALIGN(&stream->ios[Max(1, max_ios)]);

    // Enable prefetch if supported and not using direct I/O
    #ifdef USE_PREFETCH
    if ((io_direct_flags & IO_DIRECT_DATA) == 0 &&
        (flags & READ_STREAM_SEQUENTIAL) == 0 && max_ios > 0)
        stream->advice_enabled = true;
    #endif

    // Initialize stream parameters
    stream->max_ios = (max_ios == 0) ? 1 : max_ios;
    stream->io_combine_limit = io_combine_limit;
    stream->per_buffer_data_size = per_buffer_data_size;
    stream->max_pinned_buffers = max_pinned_buffers;
    stream->queue_size = queue_size;
    stream->callback = callback;
    stream->callback_private_data = callback_private_data;
    stream->buffered_blocknum = InvalidBlockNumber;

    // Set initial read distance (ramp-up vs full relation scan)
    stream->distance = (flags & READ_STREAM_FULL) ?
                       Min(max_pinned_buffers, stream->io_combine_limit) : 1;

    // Pre-initialize I/O operation structures
    for (int i = 0; i < stream->max_ios; ++i) {
        stream->ios[i].op.rel = rel;
        stream->ios[i].op.smgr = smgr;
        stream->ios[i].op.forknum = forknum;
        stream->ios[i].op.strategy = strategy;
    }

    return stream;
}
```
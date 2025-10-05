# sts_read_tuple

## Location
[src/backend/utils/sort/sharedtuplestore.c:415-494](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/utils/sort/sharedtuplestore.c#L415-L494)

## Overview
A static function that reads a tuple and its metadata from a shared tuplestore file, handling both normal-sized and oversized tuples that span multiple chunks.

## Definition
static MinimalTuple sts_read_tuple(SharedTuplestoreAccessor *accessor, void *meta_data)

## Detailed Description
This internal function reads a single tuple from the shared tuplestore file associated with the given accessor. It implements sophisticated logic to handle tuples of varying sizes:

1. **Metadata Reading**: If metadata is configured for the tuplestore, it reads the metadata first into the provided buffer.

2. **Size Reading**: Reads the tuple size (uint32) to determine how much data to read.

3. **Buffer Management**: Dynamically grows the read buffer if the incoming tuple exceeds the current buffer size, using a growth strategy that doubles the buffer size or uses the tuple size, whichever is larger.

4. **Normal Tuple Reading**: For tuples that fit within a single chunk, reads the tuple data directly into the buffer.

5. **Overflow Handling**: For oversized tuples that span multiple chunks:
   - Reads the first portion from the current chunk
   - Iterates through overflow chunks, reading chunk headers and data
   - Validates that overflow chunks are properly marked
   - Manages read position and byte counters across chunks
   - Handles tuple counting for subsequent tuples in overflow chunks

## Parameters / Member Variables
- `accessor`: A pointer to the SharedTuplestoreAccessor containing the read state and file handle
- `meta_data`: A buffer to receive metadata of the size specified during sts_initialize(); ignored if meta_data_size is 0

## Dependencies
- Functions called/Symbols referenced:
  - [SharedTuplestoreAccessor](../S/SharedTuplestoreAccessor.md), SharedTuplestoreChunk (structure types)
  - MinimalTuple (tuple type)
  - [BufFileReadExact](../B/BufFileReadExact.md) (function to read exact bytes from buffer file)
  - [MemoryContextAlloc](../M/MemoryContextAlloc.md) (memory allocation function)
  - STS_CHUNK_PAGES, STS_CHUNK_HEADER_SIZE (constants)
  - [errdetail_internal](../e/errdetail_internal.md) (error reporting function)
- Called from (representative examples):
  - [sts_parallel_scan_next](sts_parallel_scan_next.md) (in sharedtuplestore.c:505)

## Notes and Other Information
- This is a static (internal) function, not part of the public API
- Dynamically manages read buffer size to accommodate large tuples efficiently
- Properly handles the complex overflow chunk mechanism for oversized tuples
- Maintains accurate byte and tuple counters for proper file positioning
- Includes error checking for malformed overflow chunks
- Essential component of the parallel scanning infrastructure in PostgreSQL
- The returned MinimalTuple points to data in the accessors read buffer, which may be invalidated by subsequent reads
- Handles both metadata and tuple data in a single read operation for efficiency

## Simplified Source

```c
static MinimalTuple sts_read_tuple(SharedTuplestoreAccessor *accessor, void *meta_data) {
    MinimalTuple tuple;
    uint32 size;
    size_t remaining_size;
    size_t this_chunk_size;
    char *destination;

    // Read metadata if configured
    if (accessor->sts->meta_data_size > 0) {
        BufFileReadExact(accessor->read_file, meta_data, accessor->sts->meta_data_size);
        accessor->read_bytes += accessor->sts->meta_data_size;
    }

    // Read tuple size
    BufFileReadExact(accessor->read_file, &size, sizeof(size));
    accessor->read_bytes += sizeof(size);

    // Ensure read buffer is large enough
    if (size > accessor->read_buffer_size) {
        if (accessor->read_buffer != NULL)
            pfree(accessor->read_buffer);

        size_t new_size = Max(size, accessor->read_buffer_size * 2);
        accessor->read_buffer = MemoryContextAlloc(accessor->context, new_size);
        accessor->read_buffer_size = new_size;
    }

    // Read tuple data from current chunk
    remaining_size = size - sizeof(uint32);
    this_chunk_size = Min(remaining_size,
                         BLCKSZ * STS_CHUNK_PAGES - accessor->read_bytes);
    destination = accessor->read_buffer + sizeof(uint32);

    BufFileReadExact(accessor->read_file, destination, this_chunk_size);
    accessor->read_bytes += this_chunk_size;
    remaining_size -= this_chunk_size;
    destination += this_chunk_size;
    ++accessor->read_ntuples;

    // Handle overflow chunks for large tuples
    while (remaining_size > 0) {
        SharedTuplestoreChunk chunk_header;

        // Read overflow chunk header
        BufFileReadExact(accessor->read_file, &chunk_header, STS_CHUNK_HEADER_SIZE);
        accessor->read_bytes = STS_CHUNK_HEADER_SIZE;

        if (chunk_header.overflow == 0)
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("unexpected chunk in shared tuplestore temporary file")));

        // Read overflow data
        accessor->read_next_page += STS_CHUNK_PAGES;
        this_chunk_size = Min(remaining_size,
                             BLCKSZ * STS_CHUNK_PAGES - STS_CHUNK_HEADER_SIZE);

        BufFileReadExact(accessor->read_file, destination, this_chunk_size);
        accessor->read_bytes += this_chunk_size;
        remaining_size -= this_chunk_size;
        destination += this_chunk_size;

        // Reset counters for subsequent tuples in overflow chunk
        accessor->read_ntuples = 0;
        accessor->read_ntuples_available = chunk_header.ntuples;
    }

    // Prepare and return tuple
    tuple = (MinimalTuple) accessor->read_buffer;
    tuple->t_len = size;

    return tuple;
}
```
# libpq_queue_fetch_range

## Location
[src/bin/pg_rewind/libpq_source.c:356-420](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L356-L420)

## Overview
Queues a request to fetch a specific byte range from a file on the remote PostgreSQL system, with intelligent request merging and chunking capabilities.

## Definition

```c
struct
	 * the string representations of them.
	 */
	resetStringInfo(&src->paths);
```
## Detailed Description
This function implements the core logic for queuing byte-range file fetch requests from remote PostgreSQL servers during pg_rewind operations. It provides sophisticated request optimization through merging consecutive requests and automatic chunking of large requests.

The function first attempts to merge the new request with the previous one if they are contiguous (same file, consecutive byte ranges) and the previous request hasn't reached the maximum chunk size. This optimization reduces the number of network requests and improves transfer efficiency.

For requests that cannot be merged or exceed the maximum chunk size, the function automatically splits them into multiple chunks of MAX_CHUNK_SIZE bytes each. When the request queue reaches its capacity (MAX_CHUNKS_PER_QUERY), it triggers immediate processing of all queued requests to prevent queue overflow.

The function uses pointer equality for path comparison, which is sufficient for its use case since callers consistently pass the same pointer for identical file paths.

## Parameters / Member Variables
- : Pointer to the rewind_source structure containing the libpq connection and request queue
- : File path string (compared by pointer equality for request merging optimization)  
- : Byte offset within the file where the range should start
- : Number of bytes to fetch from the specified offset

## Dependencies
- Functions called/Symbols referenced:
  - [process_queued_fetch_requests](../p/process_queued_fetch_requests.md) (processes the queue when it becomes full)
  - Min (macro to find minimum of two values)
  - MAX_CHUNK_SIZE (constant defining maximum bytes per chunk)
  - MAX_CHUNKS_PER_QUERY (constant defining maximum requests per query batch)
  - [fetch_range_request](../f/fetch_range_request.md) (struct type for individual fetch requests)
- Called from:
  - [libpq_queue_fetch_file](libpq_queue_fetch_file.md) (for complete file fetch operations)
  - [init_libpq_source](../i/init_libpq_source.md) (as part of libpq_source function table initialization)

## Notes and Other Information
- The function uses pointer equality comparison for file paths, which works correctly given consistent pointer usage by callers but might miss merge opportunities if different pointers to identical strings were used
- Request merging only occurs with the immediately previous request, not with arbitrary requests in the queue
- Large requests are automatically chunked to prevent memory and network issues
- The queue size is limited to prevent unbounded memory usage and ensure reasonable batch sizes for network operations
- This is a static function used internally within the libpq_source.c module
- The function handles the balance between request efficiency (through merging) and memory management (through chunking and queue limits)

## Simplified Source

```c
static void
libpq_queue_fetch_range(rewind_source *source, const char *path, off_t off, size_t len)
{
    libpq_source *src = (libpq_source *) source;

    // Try to merge with previous request if contiguous
    if (src->num_requests > 0) {
        fetch_range_request *prev = &src->request_queue[src->num_requests - 1];

        if (prev->offset + prev->length == off &&
            prev->length < MAX_CHUNK_SIZE &&
            prev->path == path) {
            // Extend previous request up to MAX_CHUNK_SIZE
            size_t thislen = Min(len, MAX_CHUNK_SIZE - prev->length);
            prev->length += thislen;
            off += thislen;
            len -= thislen;
        }
    }

    // Split remaining data into chunks
    while (len > 0) {
        // Process queue if full
        if (src->num_requests == MAX_CHUNKS_PER_QUERY)
            process_queued_fetch_requests(src);

        // Add new chunk request
        int32 thislen = Min(len, MAX_CHUNK_SIZE);
        src->request_queue[src->num_requests].path = path;
        src->request_queue[src->num_requests].offset = off;
        src->request_queue[src->num_requests].length = thislen;
        src->num_requests++;

        off += thislen;
        len -= thislen;
    }
}
```
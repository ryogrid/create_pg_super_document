# fetch_range_request

## Location
src/bin/pg_rewind/libpq_source.c: 35 - 53

## Overview
A struct that represents a request to fetch a specific piece of a file from the source in PostgreSQL's pg_rewind utility, used for efficiently queuing file data retrieval operations.

## Definition


## Detailed Description
The `fetch_range_request` structure is a fundamental component of the libpq-based source implementation in pg_rewind. It encapsulates the information needed to request a specific range of bytes from a file on the remote PostgreSQL server. This structure is designed to support efficient batching of file retrieval operations by allowing multiple fetch requests to be queued and processed together.

The structure is used within the `libpq_source` context where an array of these requests (up to `MAX_CHUNKS_PER_QUERY` = 1000) can be queued before being sent to the remote server in a single batch operation. This batching mechanism reduces network round trips and improves the overall performance of the pg_rewind operation.

## Parameters / Member Variables
- `path`: A pointer to a string containing the file path relative to the PostgreSQL data directory root. The caller maintains the lifetime of this string.
- `offset`: The byte offset within the file where the requested range begins (type `off_t` for large file support).
- `length`: The number of bytes to fetch starting from the offset position (type `size_t`).

## Dependencies
- Functions called/Symbols referenced:
  - [rewind_source](../r/rewind_source.md) (interface)
  - MAX_CHUNKS_PER_QUERY (constant)
- Used by:
  - [libpq_queue_fetch_range](../l/libpq_queue_fetch_range.md) (queues fetch requests)
  - [process_queued_fetch_requests](../p/process_queued_fetch_requests.md) (processes queued requests)

## Notes and Other Information
- The structure is used as part of a request queue within the `libpq_source` struct, allowing up to 1000 concurrent fetch requests
- [Path](../P/Path.md) comparison uses pointer equality for optimization, relying on the caller to provide the same pointer for identical file paths
- The implementation includes logic to merge adjacent requests for the same file to optimize data transfer
- Each individual request is limited to `MAX_CHUNK_SIZE` (1MB) to prevent excessive memory usage
- Part of the pg_rewind utility's libpq-based data source implementation for synchronizing PostgreSQL data directories
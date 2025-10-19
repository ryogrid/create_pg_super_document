# libpq_queue_fetch_file

## Location
[src/bin/pg_rewind/libpq_source.c:326-355](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L326-L355)

## Overview
Queues a request to fetch an entire file from the remote PostgreSQL system during pg_rewind operations, handling the file preparation and delegating the actual fetch request.

## Definition

```c
static void
libpq_queue_fetch_file(rewind_source *source, const char *path, size_t len)
```
## Detailed Description
This function is responsible for preparing and queuing a complete file fetch operation from the remote PostgreSQL server during pg_rewind. It implements a two-step process: first truncating the target file locally, then queuing a range fetch request for the entire file content.

The function includes intelligent size handling to deal with potential race conditions where files might grow between the time they are scanned and when they are fetched. For small files (smaller than MAX_CHUNK_SIZE), it requests a full chunk size to ensure complete file retrieval even if the file has grown slightly on the source system. For larger files, it fetches up to the originally scanned size.

The function acknowledges inherent race conditions in file copying operations but follows the same approach as pg_basebackup, which has proven adequate in practice.

## Parameters / Member Variables
- `*source`: Pointer to the rewind_source structure containing connection and state information for the remote system
- `*path`: String containing the file path relative to the PostgreSQL data directory that should be fetched
- `len`: Size in bytes of the file as determined during directory scanning
## Dependencies
- Functions called/Symbols referenced:
  - [open_target_file](../o/open_target_file.md) (prepares the local target file by truncating it)
  - [libpq_queue_fetch_range](libpq_queue_fetch_range.md) (queues the actual data transfer request)
  - Max (macro to determine maximum between two values)
  - MAX_CHUNK_SIZE (constant defining the maximum chunk size for transfers)
- Called from:
  - [init_libpq_source](../i/init_libpq_source.md) (as part of libpq_source function table initialization)

## Notes and Other Information
- The function immediately truncates the target file before queuing the fetch, which simplifies tracking of complete file operations vs. partial range requests
- For files smaller than MAX_CHUNK_SIZE, it deliberately requests more data than the original file size to handle files that may have grown between scanning and fetching
- Race conditions are acknowledged but considered acceptable, following the precedent set by pg_basebackup
- This is a static function used internally within the libpq_source.c module as part of pg_rewind's remote file transfer capabilities
- The approach prioritizes simplicity over perfect consistency, which is appropriate for the pg_rewind use case where some inconsistency is acceptable and corrected by WAL replay

## Simplified Source

```c
static void
libpq_queue_fetch_file(rewind_source *source, const char *path, size_t len)
{
    // Prepare target file by truncating it
    open_target_file(path, true);

    // Queue fetch request for entire file
    // For small files, request full chunk size to handle potential growth
    size_t fetch_size = Max(len, MAX_CHUNK_SIZE);
    libpq_queue_fetch_range(source, path, 0, fetch_size);
}
```
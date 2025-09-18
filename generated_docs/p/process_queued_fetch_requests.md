# process_queued_fetch_requests

## Location
src/bin/pg_rewind/libpq_source.c: 427 - 613

## Overview
Executes all queued file fetch requests by sending a batch query to the remote PostgreSQL server and processing the returned file chunks, writing them to the target data directory.

## Definition


## Detailed Description
This function represents the core of the libpq-based file transfer mechanism in pg_rewind. It processes all queued fetch requests in a single batch operation, which significantly improves efficiency compared to individual requests.

The function constructs three PostgreSQL arrays (paths, offsets, lengths) containing the parameters for all queued requests, then executes a prepared statement ('fetch_chunks_stmt') to retrieve all the requested file chunks in one query. It uses PostgreSQL's single row mode to handle large result sets efficiently without consuming excessive memory.

The function processes each returned chunk by validating the response format and data integrity, then writes the chunk to the appropriate target file. It includes comprehensive error checking to ensure data consistency and handles special cases like deleted files (indicated by NULL chunks) by removing them from the target system.

After processing all chunks, the function validates that the number of received chunks matches the number of requests and resets the request queue for subsequent operations.

## Parameters / Member Variables
- : Pointer to the libpq_source structure containing the connection, request queue, and working buffers

## Dependencies
- Functions called/Symbols referenced:
  - pg_log_debug (logging for debugging information)
  - resetStringInfo (clears StringInfo buffers for reuse)
  - appendStringInfoChar/appendStringInfo (builds query parameter strings)
  - appendArrayEscapedString (safely escapes path strings for PostgreSQL arrays)
  - PQsendQueryPrepared (sends the prepared statement with parameters)
  - PQsetSingleRowMode (enables single row mode for memory efficiency)
  - PQgetResult (retrieves query results)
  - PQresultStatus/PQnfields/PQntuples (validates result format)
  - PQftype/PQfformat (checks result data types and formats)
  - PQgetisnull/PQgetvalue/PQgetlength (extracts data from results)
  - pg_ntoh64 (converts network byte order to host byte order)
  - open_target_file (prepares target file for writing)
  - write_target_range (writes chunk data to target file)
  - remove_target_file (removes files that were deleted on source)
  - pg_malloc/pg_free (memory management)
  - pg_fatal (reports fatal errors)
- Called from:
  - libpq_queue_fetch_range (when request queue becomes full)
  - libpq_finish_fetch (to process remaining requests at completion)

## Notes and Other Information
- Uses PostgreSQL array parameters to batch multiple requests efficiently
- Implements comprehensive validation of result format, data types, and content
- Handles deleted files by removing them from the target system when NULL chunks are received
- Validates that received chunks match exactly what was requested (path, offset, size)
- Uses single row mode to process large result sets without excessive memory usage
- Includes extensive error checking and meaningful error messages for troubleshooting
- Allows receiving less data than requested (file truncation) but not more
- Resets the request queue after successful processing to prepare for subsequent batches
- This is a static function used internally within the libpq_source.c module
- Critical for ensuring data integrity during the pg_rewind file synchronization process
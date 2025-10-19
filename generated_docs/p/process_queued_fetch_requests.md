# process_queued_fetch_requests

## Location
[src/bin/pg_rewind/libpq_source.c:427-613](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L427-L613)

## Overview
Executes all queued file fetch requests by sending a batch query to the remote PostgreSQL server and processing the returned file chunks, writing them to the target data directory.

## Definition

```c
struct
	 * the string representations of them.
	 */
	resetStringInfo(&src->paths);
```
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
  - [resetStringInfo](../r/resetStringInfo.md) (clears StringInfo buffers for reuse)
  - [appendStringInfoChar](../a/appendStringInfoChar.md)/appendStringInfo (builds query parameter strings)
  - [appendArrayEscapedString](../a/appendArrayEscapedString.md) (safely escapes path strings for PostgreSQL arrays)
  - [PQsendQueryPrepared](../P/PQsendQueryPrepared.md) (sends the prepared statement with parameters)
  - [PQsetSingleRowMode](../P/PQsetSingleRowMode.md) (enables single row mode for memory efficiency)
  - [PQgetResult](../P/PQgetResult.md) (retrieves query results)
  - [PQresultStatus](../P/PQresultStatus.md)/PQnfields/PQntuples (validates result format)
  - [PQftype](../P/PQftype.md)/PQfformat (checks result data types and formats)
  - [PQgetisnull](../P/PQgetisnull.md)/PQgetvalue/PQgetlength (extracts data from results)
  - pg_ntoh64 (converts network byte order to host byte order)
  - [open_target_file](../o/open_target_file.md) (prepares target file for writing)
  - [write_target_range](../w/write_target_range.md) (writes chunk data to target file)
  - [remove_target_file](../r/remove_target_file.md) (removes files that were deleted on source)
  - [pg_malloc](pg_malloc.md)/pg_free (memory management)
  - [pg_fatal](pg_fatal.md) (reports fatal errors)
- Called from:
  - [libpq_queue_fetch_range](../l/libpq_queue_fetch_range.md) (when request queue becomes full)
  - [libpq_finish_fetch](../l/libpq_finish_fetch.md) (to process remaining requests at completion)

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

## Simplified Source

```c
static void
process_queued_fetch_requests(libpq_source *src)
{
    const char *params[3];
    PGresult *res;
    int chunkno;

    if (src->num_requests == 0)
        return;

    pg_log_debug("getting %d file chunks", src->num_requests);

    // Build parameter arrays for prepared statement
    resetStringInfo(&src->paths);
    resetStringInfo(&src->offsets);
    resetStringInfo(&src->lengths);

    // Format as PostgreSQL arrays: {path1,path2...}, {off1,off2...}, {len1,len2...}
    appendStringInfoChar(&src->paths, '{');
    appendStringInfoChar(&src->offsets, '{');
    appendStringInfoChar(&src->lengths, '{');

    for (int i = 0; i < src->num_requests; i++)
    {
        fetch_range_request *rq = &src->request_queue[i];

        if (i > 0)
        {
            appendStringInfoChar(&src->paths, ',');
            appendStringInfoChar(&src->offsets, ',');
            appendStringInfoChar(&src->lengths, ',');
        }

        appendArrayEscapedString(&src->paths, rq->path);
        appendStringInfo(&src->offsets, INT64_FORMAT, (int64) rq->offset);
        appendStringInfo(&src->lengths, INT64_FORMAT, (int64) rq->length);
    }
    appendStringInfoChar(&src->paths, '}');
    appendStringInfoChar(&src->offsets, '}');
    appendStringInfoChar(&src->lengths, '}');

    // Execute prepared statement with array parameters
    params[0] = src->paths.data;
    params[1] = src->offsets.data;
    params[2] = src->lengths.data;

    if (PQsendQueryPrepared(src->conn, "fetch_chunks_stmt", 3, params, NULL, NULL, 1) != 1)
        pg_fatal("could not send query: %s", PQerrorMessage(src->conn));

    if (PQsetSingleRowMode(src->conn) != 1)
        pg_fatal("could not set libpq connection to single row mode");

    // Process each returned chunk
    chunkno = 0;
    while ((res = PQgetResult(src->conn)) != NULL)
    {
        fetch_range_request *rq = &src->request_queue[chunkno];
        char *filename;
        int filenamelen;
        int64 chunkoff;
        int chunksize;
        char *chunk;

        // Handle different result types
        switch (PQresultStatus(res))
        {
            case PGRES_SINGLE_TUPLE:
                break;
            case PGRES_TUPLES_OK:
                PQclear(res);
                continue;    // Final zero-row result
            default:
                pg_fatal("unexpected result while fetching remote files: %s",
                         PQresultErrorMessage(res));
        }

        // Basic validation
        if (chunkno > src->num_requests)
            pg_fatal("received more data chunks than requested");

        // Extract chunk data from result
        memcpy(&chunkoff, PQgetvalue(res, 0, 1), sizeof(int64));
        chunkoff = pg_ntoh64(chunkoff);
        chunksize = PQgetlength(res, 0, 2);

        filenamelen = PQgetlength(res, 0, 0);
        filename = pg_malloc(filenamelen + 1);
        memcpy(filename, PQgetvalue(res, 0, 0), filenamelen);
        filename[filenamelen] = '\0';

        chunk = PQgetvalue(res, 0, 2);

        // Handle file deletion (NULL chunk) or write chunk data
        if (PQgetisnull(res, 0, 2))
        {
            pg_log_debug("received null value for chunk for file \"%s\", file has been deleted",
                         filename);
            remove_target_file(filename, true);
        }
        else
        {
            pg_log_debug("received chunk for file \"%s\", offset %lld, size %d",
                         filename, (long long int) chunkoff, chunksize);

            // Validate chunk matches request
            if (strcmp(filename, rq->path) != 0)
                pg_fatal("received data for file \"%s\", when requested for \"%s\"",
                         filename, rq->path);
            if (chunkoff != rq->offset)
                pg_fatal("received data at offset %lld of file \"%s\", when requested for offset %lld",
                         (long long int) chunkoff, rq->path, (long long int) rq->offset);

            // Write chunk to target file
            open_target_file(filename, false);
            write_target_range(chunk, chunkoff, chunksize);
        }

        pg_free(filename);
        PQclear(res);
        chunkno++;
    }

    // Validate all requests were processed
    if (chunkno != src->num_requests)
        pg_fatal("unexpected number of data chunks received");

    src->num_requests = 0;
}
```
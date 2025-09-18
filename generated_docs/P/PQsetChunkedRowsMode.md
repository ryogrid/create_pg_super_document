# PQsetChunkedRowsMode

## Location
src/interfaces/libpq/fe-exec.c: 1965 - 1983

## Overview
PQsetChunkedRowsMode enables chunked results processing mode for a PostgreSQL connection, allowing results to be retrieved in configurable-sized chunks instead of waiting for the complete result set.

## Definition


## Detailed Description
PQsetChunkedRowsMode is a public libpq function that switches the connection to chunked rows mode, where query results are delivered in chunks of a specified number of rows rather than as complete result sets. This mode provides a balance between memory efficiency and performance, allowing clients to process large result sets in manageable chunks without the overhead of single-row mode.

When chunked rows mode is enabled, the client calls PQgetResult() repeatedly to retrieve each chunk. Each chunk is returned as a separate PGresult object containing up to chunkSize rows (the final chunk may contain fewer rows if the total row count is not evenly divisible by chunkSize). The final call to PQgetResult() will return NULL to indicate the end of results.

The function validates that it's safe to change the result mode using canChangeResultMode() and that the chunk size is positive before making the switch. Like single-row mode, it can only be called after a query has been sent but before any results have been retrieved.

## Parameters / Member Variables
- : The PostgreSQL connection handle
- : The maximum number of rows to include in each result chunk (must be > 0)

## Dependencies
- Functions called/Symbols referenced:
  - [canChangeResultMode](../c/canChangeResultMode.md)
- Called from (representative examples):
  - [ExecQueryAndProcessResults](../E/ExecQueryAndProcessResults.md) (in psql)
  - [test_singlerowmode](../t/test_singlerowmode.md) (in libpq_pipeline tests)

## Notes and Other Information
- Returns 1 for success, 0 for failure (including invalid chunkSize <= 0)
- Sets conn->partialResMode = true to enable partial result processing
- Sets conn->singleRowMode = false to distinguish from single-row mode
- Sets conn->maxChunkSize = chunkSize to control the chunk size
- Must be called after sending a query but before retrieving any results
- Provides better performance than single-row mode for most applications while still managing memory usage
- Part of libpq's partial result mode system for handling large datasets efficiently
- The chunk size parameter allows tuning the balance between memory usage and network efficiency
- Cannot be used with already-retrieved results or when no query is active
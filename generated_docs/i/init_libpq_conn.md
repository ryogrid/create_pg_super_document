# init_libpq_conn

## Location
src/bin/pg_rewind/libpq_source.c: 111 - 163

## Overview
Initializes and configures a PostgreSQL connection for safe use by pg_rewind, setting appropriate timeouts, read-only mode, and preparing necessary statements.

## Definition


## Detailed Description
The  function prepares a PostgreSQL connection for use by pg_rewind by configuring various connection settings and validating server requirements. This function ensures the connection operates safely and efficiently during the rewind process.

The function performs several critical initialization steps:
1. Disables all timeout settings to prevent interruptions during long-running operations
2. Sets the connection to read-only mode for safety
3. Secures the search_path to prevent potential security issues
4. Validates that full_page_writes is enabled (required for torn page protection)
5. Prepares a parameterized statement for efficient batch file fetching

The prepared statement uses PostgreSQL's  function with  to fetch multiple file chunks in a single query, which is essential for performance when dealing with many files.

## Parameters
- : An established PGconn connection to be initialized and configured for pg_rewind operations

## Dependencies
- Functions called/Symbols referenced:
  - [run_simple_command](../r/run_simple_command.md)
  - [run_simple_query](../r/run_simple_query.md)
  - [PQexec](../P/PQexec.md)
  - [PQprepare](../P/PQprepare.md)
  - [PQresultStatus](../P/PQresultStatus.md)
  - [PQresultErrorMessage](../P/PQresultErrorMessage.md)
  - [PQclear](../P/PQclear.md)
  - [pg_free](../p/pg_free.md)
  - [pg_fatal](../p/pg_fatal.md)
  - ALWAYS_SECURE_SEARCH_PATH_SQL
  - PGRES_TUPLES_OK
  - PGRES_COMMAND_OK
- Called from:
  - [init_libpq_source](init_libpq_source.md) (in src/bin/pg_rewind/libpq_source.c:86)

## Notes and Other Information
- This is a static function, only accessible within the libpq_source.c file
- The function requires full_page_writes to be enabled on the source server to handle torn pages that might occur during concurrent reads
- All timeout settings are disabled to prevent interruptions during potentially long-running file transfer operations
- The prepared statement 'fetch_chunks_stmt' is used later for efficient batch file retrieval
- Read-only mode provides an additional safety layer to prevent accidental modifications during the rewind process
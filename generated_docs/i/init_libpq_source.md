# init_libpq_source

## Location
[src/bin/pg_rewind/libpq_source.c:82-110](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/libpq_source.c#L82-L110)

## Overview
Creates and initializes a libpq data source for the pg_rewind utility, establishing a connection-based source that can traverse files and fetch data from a remote PostgreSQL instance.

## Definition


## Detailed Description
The  function creates a new libpq-based data source used by pg_rewind to connect to and fetch data from a remote PostgreSQL server. It takes an already-established PostgreSQL connection and wraps it in a  structure that implements the generic  interface.

The function initializes the source by:
1. Calling  to prepare the connection
2. Allocating memory for the libpq_source structure
3. Setting up function pointers for various operations (traverse, fetch, queue operations)
4. Initializing StringInfo structures for managing paths, offsets, and lengths
5. Storing the connection reference

This allows pg_rewind to treat both local file system sources and remote libpq sources uniformly through the same interface.

## Parameters
- : Pre-established PGconn connection to the remote PostgreSQL server. The caller should not use this connection while the source is active.

## Dependencies
- Functions called/Symbols referenced:
  - [init_libpq_conn](init_libpq_conn.md)
  - pg_malloc0
  - [libpq_traverse_files](../l/libpq_traverse_files.md)
  - [libpq_fetch_file](../l/libpq_fetch_file.md)  
  - [libpq_queue_fetch_file](../l/libpq_queue_fetch_file.md)
  - [libpq_queue_fetch_range](../l/libpq_queue_fetch_range.md)
  - [libpq_finish_fetch](../l/libpq_finish_fetch.md)
  - [libpq_get_current_wal_insert_lsn](../l/libpq_get_current_wal_insert_lsn.md)
  - [libpq_destroy](../l/libpq_destroy.md)
  - initStringInfo
- Called from:
  - [main](../m/main.md) (in src/bin/pg_rewind/pg_rewind.c:316)

## Notes and Other Information
- The function is part of pg_rewind's abstraction layer that allows the tool to work with both local and remote data sources
- The caller retains ownership of the PGconn but should not use it directly while the source is active
- The returned source must eventually be destroyed using the libpq_destroy function
- All StringInfo structures (paths, offsets, lengths) are initialized to manage batched operations efficiently
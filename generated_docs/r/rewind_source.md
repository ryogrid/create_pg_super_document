# rewind_source

## Location
src/bin/pg_rewind/rewind_source.h: 23 - 78

## Overview
The rewind_source struct defines an abstract interface for data sources used by pg_rewind utility, providing a unified API for accessing files and WAL data from both local and remote PostgreSQL servers.

## Definition


## Detailed Description
The rewind_source struct serves as a polymorphic interface in the pg_rewind utility, enabling access to PostgreSQL server data through different implementation strategies. This design allows pg_rewind to work with both local file system access and remote connections via libpq. The struct contains function pointers that define the complete contract for data retrieval operations needed during the rewind process, including file traversal, content fetching, and WAL position queries.

The interface supports both immediate and deferred execution models through queuing mechanisms, which is particularly important for optimizing network operations when working with remote servers. All implementations must provide complete functionality for file operations, WAL access, and proper resource cleanup.

## Parameters / Member Variables
- : Function pointer to traverse all files in the source data directory, calling a callback on each file
- : Function pointer to fetch a complete file into a malloc'd buffer with size information
- : Function pointer to queue a request for fetching part of a file (offset and length specified)
- : Function pointer to queue a request for replacing a whole local file from source
- : Function pointer to execute all queued fetch requests
- : Function pointer to get the current WAL insert position from the source system
- : Function pointer to free the rewind_source object and associated resources

## Dependencies
- Functions called/Symbols referenced:
  - process_file_callback_t (callback type for file traversal)
  - XLogRecPtr (WAL position type)
- Called from (representative examples):
  - libpq_source (remote server implementation)
  - local_source (local filesystem implementation)
  - [perform_rewind](../p/perform_rewind.md) (main rewind operation)

## Notes and Other Information
This struct implements the Strategy pattern, allowing pg_rewind to work with different data sources transparently. Two main implementations exist: local_source for direct filesystem access and libpq_source for remote PostgreSQL connections. The queuing mechanism in queue_fetch_range and queue_fetch_file is designed to optimize batch operations, particularly important for network efficiency with remote sources. The interface ensures that all file operations are properly abstracted, making the rewind logic independent of whether the source is local or remote.
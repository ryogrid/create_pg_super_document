# init_local_source

## Location
src/bin/pg_rewind/local_source.c: 40 - 59

## Overview
Initializes a local source for pg_rewind operations, setting up function pointers to local file operations and returning a rewind_source interface.

## Definition
rewind_source *init_local_source(const char *datadir)

## Detailed Description
This function creates and initializes a local_source structure, which is used by pg_rewind to access files from the local PostgreSQL data directory during the rewinding process. The function allocates memory for a local_source structure, populates its function pointers with local file operation functions, stores the data directory path, and returns a pointer to the common rewind_source interface.

The local source provides an abstraction layer that allows pg_rewind to treat local file system operations uniformly with remote operations, following a common interface pattern.

## Parameters / Member Variables
- `datadir`: Path to the PostgreSQL data directory that will serve as the source for file operations

## Dependencies
- Functions called/Symbols referenced:
  - pg_malloc0
  - local_traverse_files
  - local_fetch_file
  - local_queue_fetch_file
  - local_queue_fetch_range
  - local_finish_fetch
  - local_destroy
- Called from (representative examples):
  - main (in src/bin/pg_rewind/pg_rewind.c:319)

## Notes and Other Information
- This function is part of the pg_rewind utility's source abstraction layer
- The returned structure implements the rewind_source interface, allowing uniform treatment of local and remote sources
- Memory is allocated using pg_malloc0, ensuring zero-initialization
- The get_current_wal_insert_lsn function pointer is set to NULL for local sources since WAL insert LSN is not applicable for local file operations
- The function is located in src/bin/pg_rewind/local_source.c:40-59
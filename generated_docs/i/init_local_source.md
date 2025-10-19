# init_local_source

## Location
[src/bin/pg_rewind/local_source.c:40-59](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/local_source.c#L40-L59)

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
  - [pg_malloc0](../p/pg_malloc0.md)
  - [local_traverse_files](../l/local_traverse_files.md)
  - [local_fetch_file](../l/local_fetch_file.md)
  - [local_queue_fetch_file](../l/local_queue_fetch_file.md)
  - [local_queue_fetch_range](../l/local_queue_fetch_range.md)
  - [local_finish_fetch](../l/local_finish_fetch.md)
  - [local_destroy](../l/local_destroy.md)
- Called from (representative examples):
  - [main](../m/main.md) (in src/bin/pg_rewind/pg_rewind.c:319)

## Notes and Other Information
- This function is part of the pg_rewind utility's source abstraction layer
- The returned structure implements the rewind_source interface, allowing uniform treatment of local and remote sources
- Memory is allocated using pg_malloc0, ensuring zero-initialization
- The get_current_wal_insert_lsn function pointer is set to NULL for local sources since WAL insert LSN is not applicable for local file operations
- The function is located in src/bin/pg_rewind/local_source.c:40-59

## Simplified Source

```c
rewind_source *
init_local_source(const char *datadir)
{
    // Allocate and initialize local source structure
    local_source *src = pg_malloc0(sizeof(local_source));

    // Set up function pointers for local file operations
    src->common.traverse_files = local_traverse_files;
    src->common.fetch_file = local_fetch_file;
    src->common.queue_fetch_file = local_queue_fetch_file;
    src->common.queue_fetch_range = local_queue_fetch_range;
    src->common.finish_fetch = local_finish_fetch;
    src->common.get_current_wal_insert_lsn = NULL;  // Not applicable for local
    src->common.destroy = local_destroy;

    // Store data directory path
    src->datadir = datadir;

    return &src->common;
}
```
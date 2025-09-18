# traverse_datadir

## Location
src/bin/pg_rewind/file_ops.c: 362 - 373

## Overview
Initiates a recursive traversal of all files in a PostgreSQL data directory, invoking a callback function for each file encountered.

## Definition
```c
void traverse_datadir(const char *datadir, process_file_callback_t callback)
```

## Detailed Description
This function serves as a convenient entry point for traversing all files within a PostgreSQL data directory. It acts as a wrapper around the more general recurse_dir function, providing a simplified interface for directory traversal operations. The function starts the recursive traversal from the root of the specified data directory and calls the provided callback function for each file encountered during the traversal process.

## Parameters / Member Variables
- `datadir`: Root path of the PostgreSQL data directory to traverse
- `callback`: Function pointer of type process_file_callback_t that will be called for each file found during traversal

## Dependencies
- Functions called/Symbols referenced:
  - [recurse_dir](../r/recurse_dir.md) (main recursive directory traversal function)
- Called from (representative examples):
  - [local_traverse_files](../l/local_traverse_files.md) (local_source.c:62)
  - [main](../m/main.md) (pg_rewind.c:478)
- Declared in:
  - file_ops.h:27

## Notes and Other Information
- This is a convenience wrapper that simplifies the interface to recurse_dir by passing NULL as the relative path parameter
- The actual traversal logic is implemented in recurse_dir, which handles the recursive directory walking
- Used extensively in pg_rewind for operations that need to process all files in a data directory
- The callback function type process_file_callback_t defines the interface for file processing operations
- Common use cases include file comparison, copying, and validation operations during database rewind procedures
# local_queue_fetch_range

## Location
[src/bin/pg_rewind/local_source.c:128-175](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/local_source.c#L128-L175)

## Overview
A static function that copies a specific range of bytes from a file in the local source to the target during pg_rewind operations, supporting partial file transfers.

## Definition
static void local_queue_fetch_range(rewind_source *source, const char *path, off_t off, size_t len)

## Detailed Description
This function implements the queue_fetch_range method for local sources in the rewind_source interface. It performs a partial file copy operation, transferring only a specified range of bytes from the local PostgreSQL data directory to the target location. The function opens the source file, seeks to the specified offset, and copies the requested number of bytes in chunks using an aligned buffer.

Unlike local_queue_fetch_file which copies entire files, this function is designed for incremental updates where only specific portions of a file need to be synchronized. It's particularly useful for large files where only certain blocks have changed.

## Parameters / Member Variables
- `source`: Pointer to the rewind_source structure (cast to local_source internally)
- `path`: Relative path to the file within the data directory  
- `off`: Starting offset in the file (off_t type for large file support)
- `len`: Number of bytes to copy from the offset

## Dependencies
- Functions called/Symbols referenced:
  - snprintf
  - open
  - [pg_fatal](../p/pg_fatal.md)
  - lseek
  - [open_target_file](../o/open_target_file.md)
  - read
  - [write_target_range](../w/write_target_range.md)
  - close
- Called from (representative examples):
  - Via function pointer in rewind_source interface

## Notes and Other Information
- This function is static and only used within the local_source.c file
- It's assigned to the queue_fetch_range function pointer in init_local_source
- Uses PGIOAlignedBlock for optimal I/O performance with aligned memory buffers
- Uses lseek with SEEK_SET to position at the specified offset before reading
- Opens target file without truncation (open_target_file with false parameter) for partial updates
- Handles partial reads by looping until the full requested range is copied
- Reports fatal errors for unexpected EOF, which indicates file corruption or concurrent modification
- Uses off_t type for offset to support large files (>2GB on systems with large file support)
- Calculates read chunk size dynamically, using full buffer size or remaining bytes, whichever is smaller
- Located in src/bin/pg_rewind/local_source.c:128-175
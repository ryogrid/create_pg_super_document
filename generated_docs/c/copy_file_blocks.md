# copy_file_blocks

## Location
[src/bin/pg_combinebackup/copy_file.c:160-212](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/copy_file.c#L160-L212)

## Overview
Copies a file block by block from source to destination while optionally computing a checksum during the copy operation.

## Definition

```c
static void
copy_file_blocks(const char *src, const char *dst,
				 pg_checksum_context *checksum_ctx)
```
## Detailed Description
The `copy_file_blocks` function implements a straightforward block-by-block file copying strategy with integrated checksum calculation. It opens both source and destination files, reads data in 50-block chunks, writes to the destination, and updates the checksum context for each chunk. The function includes comprehensive error handling for both read and write operations, providing detailed error messages with offset information for partial writes. This is the default fallback copying method used by pg_combinebackup when more advanced techniques like cloning or copy_file_range are not available.

## Parameters / Member Variables
- `src`: Path to the source file to copy from
- `dst`: Path to the destination file to copy to  
- `checksum_ctx`: Pointer to checksum context for incremental checksum computation

## Dependencies
- Functions called/Symbols referenced:
  - `open` - System call for file opening
  - `[pg_malloc](../p/pg_malloc.md)` - PostgreSQL memory allocation
  - `read` - System call for reading data
  - `write` - System call for writing data
  - `[pg_checksum_update](../p/pg_checksum_update.md)` - Updates checksum with copied data
  - [pg_free](../p/pg_free.md) - PostgreSQL memory deallocation
  - `close` - System call for file closing
  - `pg_file_create_mode` - File creation permissions
- Called from:
  - [copy_file](copy_file.md) (src/bin/pg_combinebackup/copy_file.c:84) - as COPY_METHOD_COPY strategy

## Notes and Other Information
- Uses 50-block buffer size (50 * BLCKSZ) for I/O efficiency
- Provides detailed error reporting including byte offsets for debugging
- Static function with module-local scope in copy_file.c
- Serves as the reliable fallback when platform-specific optimized copy methods fail
- Part of PostgreSQL's pg_combinebackup utility for incremental backup processing
- Location: src/bin/pg_combinebackup/copy_file.c:160-212
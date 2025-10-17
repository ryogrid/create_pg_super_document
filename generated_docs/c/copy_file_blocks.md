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

## Simplified Source

```c
static void
copy_file_blocks(const char *src, const char *dst,
                 pg_checksum_context *checksum_ctx)
{
    int src_fd;
    int dest_fd;
    uint8 *buffer;
    const int buffer_size = 50 * BLCKSZ;
    ssize_t bytes_read;
    unsigned offset = 0;

    // Open source file for reading
    if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
        pg_fatal("could not open file \"%s\": %m", src);

    // Open destination file for writing
    if ((dest_fd = open(dst, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY,
                        pg_file_create_mode)) < 0)
        pg_fatal("could not open file \"%s\": %m", dst);

    // Allocate copy buffer
    buffer = pg_malloc(buffer_size);

    // Copy file in chunks
    while ((bytes_read = read(src_fd, buffer, buffer_size)) > 0)
    {
        // Write chunk to destination
        ssize_t bytes_written = write(dest_fd, buffer, bytes_read);
        if (bytes_written != bytes_read)
            pg_fatal("write error to file \"%s\" at offset %u", dst, offset);

        // Update checksum with copied data
        if (pg_checksum_update(checksum_ctx, buffer, bytes_read) < 0)
            pg_fatal("could not update checksum of file \"%s\"", dst);

        offset += bytes_read;
    }

    // Check for read errors
    if (bytes_read < 0)
        pg_fatal("could not read from file \"%s\": %m", src);

    // Clean up resources
    pg_free(buffer);
    close(src_fd);
    close(dest_fd);
}
```
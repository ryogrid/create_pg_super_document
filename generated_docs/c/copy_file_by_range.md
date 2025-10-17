# copy_file_by_range

## Location
[src/bin/pg_combinebackup/copy_file.c:259-293](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/copy_file.c#L259-L293)

## Overview
Copies a file from source to destination using the copy_file_range system call for efficient data transfer, with optional checksum calculation.

## Definition

```c
static void
copy_file_by_range(const char *src, const char *dest,
				   pg_checksum_context *checksum_ctx)
```
## Detailed Description
The `copy_file_by_range` function utilizes the Linux `copy_file_range` system call to perform efficient file copying. This system call can optimize data transfer by avoiding unnecessary copies between kernel and user space, and may use advanced filesystem features like reflinks or server-side copy operations. The function repeatedly calls `copy_file_range` with `SSIZE_MAX` length until all data is copied, as the system call may not transfer the entire file in a single operation. After successful copying, it separately calculates the checksum by reading the source file if needed, since the copying doesn't provide access to the data stream for checksum computation.

## Parameters / Member Variables
- `src`: Path to the source file to copy from
- `dest`: Path to the destination file to create
- `checksum_ctx`: Pointer to checksum context for checksum calculation

## Dependencies
- Functions called/Symbols referenced:
  - `open` - System call for file opening
  - `copy_file_range` - Linux system call for efficient file copying
  - `close` - System call for file closing
  - [checksum_file](checksum_file.md) - Calculate checksum of the copied file
  - `pg_file_create_mode` - File creation permissions
  - `SSIZE_MAX` - Maximum value for ssize_t type
- Called from:
  - [copy_file](copy_file.md) (src/bin/pg_combinebackup/copy_file.c:88) - as COPY_METHOD_COPY_FILE_RANGE strategy

## Notes and Other Information
- Linux-specific optimization requiring HAVE_COPY_FILE_RANGE compile-time support
- May provide performance benefits through kernel-level optimizations and reduced context switching
- Uses a loop since copy_file_range may not copy the entire file in one call
- Checksum calculation performed separately after copying since data doesn't pass through user space
- Will fatal error on platforms without copy_file_range support
- Part of PostgreSQL's pg_combinebackup utility advanced copying strategies
- Location: src/bin/pg_combinebackup/copy_file.c:259-293

## Simplified Source

```c
static void
copy_file_by_range(const char *src, const char *dest,
                   pg_checksum_context *checksum_ctx)
{
#if defined(HAVE_COPY_FILE_RANGE)
    int src_fd;
    int dest_fd;
    ssize_t bytes_copied;

    // Open source file for reading
    if ((src_fd = open(src, O_RDONLY | PG_BINARY, 0)) < 0)
        pg_fatal("could not open file \"%s\": %m", src);

    // Create destination file
    if ((dest_fd = open(dest, O_RDWR | O_CREAT | O_EXCL | PG_BINARY,
                        pg_file_create_mode)) < 0)
        pg_fatal("could not create file \"%s\": %m", dest);

    // Copy data using copy_file_range system call
    do
    {
        bytes_copied = copy_file_range(src_fd, NULL, dest_fd, NULL, SSIZE_MAX, 0);
        if (bytes_copied < 0)
            pg_fatal("error while copying file range from \"%s\" to \"%s\": %m",
                     src, dest);
    } while (bytes_copied > 0);

    // Close files
    close(src_fd);
    close(dest_fd);

#else
    // Platform doesn't support copy_file_range
    pg_fatal("copy_file_range not supported on this platform");
#endif

    // Calculate checksum of the copied file if needed
    checksum_file(src, checksum_ctx);
}
```
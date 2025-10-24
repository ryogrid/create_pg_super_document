# copy_file

## Location
[src/bin/pg_combinebackup/copy_file.c:49-126](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_combinebackup/copy_file.c#L49-L126)

## Overview
Copies a file from source to destination using a buffered read-write operation with periodic flushing for performance optimization.

## Definition

```c
void
copy_file(const char *src, const char *dst,
		  pg_checksum_context *checksum_ctx,
		  CopyMethod copy_method, bool dry_run)
```
## Detailed Description
The  function performs a complete file copy operation using PostgreSQL's transient file management system. It reads data in 8-block chunks (COPY_BUF_SIZE = 8 * BLCKSZ) and periodically flushes the destination file to avoid overwhelming the system cache. The flush frequency is platform-dependent: every 1MB on most systems, but every 32MB on macOS due to APFS performance characteristics. The function includes comprehensive error handling and uses PostgreSQL's wait event reporting for monitoring I/O operations.

## Parameters / Member Variables
- : Source file path to copy from
- : Destination file path to copy to

## Dependencies
- Functions called/Symbols referenced:
  -  - Memory allocation
  -  - PostgreSQL file opening
  -  - PostgreSQL file closing  
  -  - Data flushing
  -  - Wait event reporting
  -  - Signal handling
  -  - Memory deallocation
- Called from (representative examples):
  -  (src/backend/storage/file/copydir.c:74)
  -  (src/backend/storage/file/reinit.c:315)
  - Various COPY command functions in copyfrom.c and copyto.c

## Notes and Other Information
- Uses platform-specific flush distances for optimal performance
- Implements interrupt checking for cancellation support
- Part of PostgreSQL's core storage file management infrastructure
- Location: src/backend/storage/file/copydir.c:117-216

## Simplified Source

```c
void
copy_file(const char *fromfile, const char *tofile)
{
    char *buffer;
    int srcfd, dstfd;
    int nbytes;
    off_t offset, flush_offset;

    /* Platform-specific flush distances for optimal performance */
#define COPY_BUF_SIZE (8 * BLCKSZ)
#if defined(__darwin__)
#define FLUSH_DISTANCE (32 * 1024 * 1024)  // 32MB for macOS/APFS
#else
#define FLUSH_DISTANCE (1024 * 1024)       // 1MB for other platforms
#endif

    // Allocate aligned buffer for I/O operations
    buffer = palloc(COPY_BUF_SIZE);

    // Open source file for reading
    srcfd = OpenTransientFile(fromfile, O_RDONLY | PG_BINARY);
    if (srcfd < 0)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not open file \"%s\": %m", fromfile)));

    // Create destination file
    dstfd = OpenTransientFile(tofile, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
    if (dstfd < 0)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not create file \"%s\": %m", tofile)));

    // Copy data in chunks with periodic flushing
    flush_offset = 0;
    for (offset = 0;; offset += nbytes) {
        // Check for cancellation signals
        CHECK_FOR_INTERRUPTS();

        // Flush data periodically to avoid cache pressure
        if (offset - flush_offset >= FLUSH_DISTANCE) {
            pg_flush_data(dstfd, flush_offset, offset - flush_offset);
            flush_offset = offset;
        }

        // Read chunk from source
        pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_READ);
        nbytes = read(srcfd, buffer, COPY_BUF_SIZE);
        pgstat_report_wait_end();

        if (nbytes < 0)
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not read file \"%s\": %m", fromfile)));
        if (nbytes == 0)
            break;  // End of file

        // Write chunk to destination
        errno = 0;
        pgstat_report_wait_start(WAIT_EVENT_COPY_FILE_WRITE);
        if ((int) write(dstfd, buffer, nbytes) != nbytes) {
            if (errno == 0) errno = ENOSPC;  // Assume disk full
            ereport(ERROR, (errcode_for_file_access(),
                           errmsg("could not write to file \"%s\": %m", tofile)));
        }
        pgstat_report_wait_end();
    }

    // Final flush of remaining data
    if (offset > flush_offset)
        pg_flush_data(dstfd, flush_offset, offset - flush_offset);

    // Close files and cleanup
    if (CloseTransientFile(dstfd) != 0)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not close file \"%s\": %m", tofile)));

    if (CloseTransientFile(srcfd) != 0)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not close file \"%s\": %m", fromfile)));

    pfree(buffer);
}
```
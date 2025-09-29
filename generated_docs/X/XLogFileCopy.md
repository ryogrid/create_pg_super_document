# XLogFileCopy

## Location
[src/backend/access/transam/xlog.c:3395-3539](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3395-L3539)

## Overview
Creates a new XLOG file segment by copying data from a pre-existing segment, supporting recovery operations where WAL segments need to be duplicated across timelines.

## Definition
static void XLogFileCopy(TimeLineID destTLI, XLogSegNo destsegno, TimeLineID srcTLI, XLogSegNo srcsegno, int upto)

## Detailed Description
XLogFileCopy is a static function used primarily during recovery operations to create new WAL segments by copying from existing ones, potentially across different timelines. The function performs a block-by-block copy operation, reading data from the source segment up to a specified point and padding the remainder with zeros. It creates the new segment in a temporary location first, then uses InstallXLogFileSegment to atomically move it to its final location.

The copying process includes comprehensive error handling, progress reporting through wait events, and ensures data integrity through fsync operations. The function is designed to work without locking since it's typically used during recovery when no concurrent access occurs.

## Parameters / Member Variables
- : TimeLineID for the destination timeline where the new segment will be created
- : XLogSegNo identifying the destination segment number
- : TimeLineID of the source timeline from which to copy
- : XLogSegNo identifying the source segment number to copy from  
- : Integer specifying how many bytes to copy from the source (remainder filled with zeros)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogFilePath](XLogFilePath.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - unlink
  - read
  - write
  - [pg_fsync](../p/pg_fsync.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - [InstallXLogFileSegment](../I/InstallXLogFileSegment.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end
  - ereport
  - [errcode_for_file_access](../e/errcode_for_file_access.md)
  - [data_sync_elevel](../d/data_sync_elevel.md)
- Called from (representative examples):
  - [XLogInitNewTimeline](XLogInitNewTimeline.md) (src/backend/access/transam/xlog.c:5207)

## Notes and Other Information
- Currently only used during recovery operations
- Uses temporary file pattern: XLOGDIR/xlogtemp.<pid> for atomic installation
- Performs block-by-block copying using PGAlignedXLogBlock buffer
- Implements comprehensive wait event reporting (WAL_COPY_READ, WAL_COPY_WRITE, WAL_COPY_SYNC)
- Source file opened read-only, destination created with O_CREAT | O_EXCL flags
- Automatically cleans up temporary file on write failures
- Does not use get_sync_bit() - only fsyncs at the end of the copy operation
- Located in src/backend/access/transam/xlog.c:3395-3539

## Simplified Source

```c
// Simplified version of XLogFileCopy
static void XLogFileCopy(TimeLineID destTLI, XLogSegNo destsegno,
                        TimeLineID srcTLI, XLogSegNo srcsegno, int upto)
{
    char path[MAXPGPATH];
    char tmppath[MAXPGPATH];
    PGAlignedXLogBlock buffer;
    int srcfd, fd, nbytes;

    // Open the source WAL segment file
    XLogFilePath(path, srcTLI, srcsegno, wal_segment_size);
    srcfd = OpenTransientFile(path, O_RDONLY | PG_BINARY);
    if (srcfd < 0)
        ereport(ERROR, "could not open source file");

    // Create temporary destination file
    snprintf(tmppath, MAXPGPATH, XLOGDIR "/xlogtemp.%d", (int) getpid());
    unlink(tmppath);  // Remove any existing temp file
    fd = OpenTransientFile(tmppath, O_RDWR | O_CREAT | O_EXCL | PG_BINARY);
    if (fd < 0)
        ereport(ERROR, "could not create temp file");

    // Copy data block by block
    for (nbytes = 0; nbytes < wal_segment_size; nbytes += sizeof(buffer))
    {
        int nread = upto - nbytes;

        // Zero-fill the buffer (for padding beyond 'upto')
        if (nread < sizeof(buffer))
            memset(buffer.data, 0, sizeof(buffer));

        // Read from source if we haven't reached the 'upto' limit
        if (nread > 0)
        {
            if (nread > sizeof(buffer))
                nread = sizeof(buffer);

            pgstat_report_wait_start(WAIT_EVENT_WAL_COPY_READ);
            if (read(srcfd, buffer.data, nread) != nread)
                ereport(ERROR, "could not read from source");
            pgstat_report_wait_end();
        }

        // Write the buffer to destination
        pgstat_report_wait_start(WAIT_EVENT_WAL_COPY_WRITE);
        if (write(fd, buffer.data, sizeof(buffer)) != sizeof(buffer))
        {
            unlink(tmppath);  // Clean up on failure
            ereport(ERROR, "could not write to temp file");
        }
        pgstat_report_wait_end();
    }

    // Sync and close files
    pgstat_report_wait_start(WAIT_EVENT_WAL_COPY_SYNC);
    pg_fsync(fd);
    pgstat_report_wait_end();

    CloseTransientFile(fd);
    CloseTransientFile(srcfd);

    // Atomically install the new segment
    if (!InstallXLogFileSegment(&destsegno, tmppath, false, 0, destTLI))
        elog(ERROR, "InstallXLogFileSegment should not have failed");
}
```

Key simplifications made:
- Consolidated error handling into single-line ereport calls
- Removed detailed error message formatting and errno handling
- Simplified variable declarations to single lines where possible
- Abstracted complex read/write error checking into basic success/failure checks
- Removed detailed error recovery logic while preserving temp file cleanup
- Maintained essential algorithm: open source, create temp, copy blocks, sync, install
- Preserved critical wait event reporting for performance monitoring
- Kept the block-by-block copying logic and zero-padding behavior intact
# XLogFileInitInternal

## Location
[src/backend/access/transam/xlog.c:3187-3356](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/xlog.c#L3187-L3356)

## Overview
Creates and initializes a new WAL file segment by either reusing an existing file or creating a zero-filled temporary file that gets atomically moved into place.

## Definition

```c
static int
XLogFileInitInternal(XLogSegNo logsegno, TimeLineID logtli,
					 bool *added, char *path)
```
## Detailed Description
XLogFileInitInternal is responsible for ensuring a specific WAL file segment exists and is properly initialized. The function implements a robust two-phase creation process:

**Phase 1 - Check for existing file:**
- First attempts to open an existing file at the target location
- If successful, returns the file descriptor immediately (checkpoint maker may have already created it)
- Uses proper sync flags based on wal_sync_method configuration

**Phase 2 - Create new file if needed:**
- Creates a temporary file with a unique name (xlogtemp.PID) to avoid conflicts
- Initializes the file content based on wal_init_zero setting:
  - If wal_init_zero=true: Zero-fills the entire segment to ensure disk space allocation
  - If wal_init_zero=false: Writes only a single byte at the end (more efficient but may create sparse files)
- Performs fsync to ensure the data reaches disk
- Atomically renames the temporary file to its final location using InstallXLogFileSegment
- Handles concurrent creation attempts gracefully by potentially using the created segment for a future slot

The function includes comprehensive error handling, wait event reporting for monitoring, and support for direct I/O optimization when configured.

## Parameters / Member Variables
- : The WAL segment number to create
- : Timeline ID for the segment
- : Output parameter set to true if a new segment was actually created
- : Output buffer (MAXPGPATH) containing the final path to the segment file
- Returns: File descriptor of opened file, or -1 (caller should open the path directly)

## Dependencies
- Functions called/Symbols referenced:
  - [XLogFilePath](XLogFilePath.md)
  - [BasicOpenFile](../B/BasicOpenFile.md)
  - [get_sync_bit](../g/get_sync_bit.md)
  - [pg_pwrite_zeros](../p/pg_pwrite_zeros.md)
  - [pg_pwrite](../p/pg_pwrite.md)
  - [pg_fsync](../p/pg_fsync.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end
  - [InstallXLogFileSegment](../I/InstallXLogFileSegment.md)
- Called from (representative examples):
  - [XLogFileInit](XLogFileInit.md)
  - [PreallocXlogFiles](../P/PreallocXlogFiles.md)

## Notes and Other Information
- Uses CheckPointSegments to determine maximum pre-created segments
- Supports direct I/O when IO_DIRECT_WAL_INIT flag is set
- Implements atomic file creation via temporary files to avoid corruption
- Handles race conditions where multiple processes may create the same segment
- The wal_init_zero setting affects both performance and disk space allocation behavior
- Returns -1 even on success; callers typically need to open the returned path
- Includes comprehensive wait event reporting for performance monitoring
- Properly cleans up temporary files on failure to prevent disk space leaks

## Simplified Source

```c
// Simplified version of XLogFileInitInternal
static int XLogFileInitInternal(XLogSegNo logsegno, TimeLineID logtli, bool *added, char *path) {
    char tmppath[MAXPGPATH];
    XLogSegNo installed_segno;
    XLogSegNo max_segno;
    int fd;
    int save_errno;
    int open_flags = O_RDWR | O_CREAT | O_EXCL | PG_BINARY;

    // Step 1: Build target path and try to open existing file
    XLogFilePath(path, logtli, logsegno, wal_segment_size);
    *added = false;

    fd = BasicOpenFile(path, O_RDWR | PG_BINARY | O_CLOEXEC | get_sync_bit(wal_sync_method));
    if (fd >= 0)
        return fd;  // File already exists, return it

    if (errno != ENOENT)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not open file \"%s\": %m", path)));

    // Step 2: Create new file via temporary file
    elog(DEBUG2, "creating and filling new WAL file");
    snprintf(tmppath, MAXPGPATH, XLOGDIR "/xlogtemp.%d", (int) getpid());
    unlink(tmppath);

    // Add direct I/O flag if configured
    if (io_direct_flags & IO_DIRECT_WAL_INIT)
        open_flags |= PG_O_DIRECT;

    // Create temporary file
    fd = BasicOpenFile(tmppath, open_flags);
    if (fd < 0)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not create file \"%s\": %m", tmppath)));

    // Step 3: Initialize file content based on configuration
    pgstat_report_wait_start(WAIT_EVENT_WAL_INIT_WRITE);
    save_errno = 0;

    if (wal_init_zero) {
        // Zero-fill entire segment for guaranteed disk allocation
        ssize_t rc = pg_pwrite_zeros(fd, wal_segment_size, 0);
        if (rc < 0)
            save_errno = errno;
    } else {
        // Write single byte at end (may create sparse file)
        errno = 0;
        if (pg_pwrite(fd, "\0", 1, wal_segment_size - 1) != 1)
            save_errno = errno ? errno : ENOSPC;
    }
    pgstat_report_wait_end();

    // Handle write errors
    if (save_errno) {
        unlink(tmppath);
        close(fd);
        errno = save_errno;
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not write to file \"%s\": %m", tmppath)));
    }

    // Step 4: Sync and close temporary file
    pgstat_report_wait_start(WAIT_EVENT_WAL_INIT_SYNC);
    if (pg_fsync(fd) != 0) {
        save_errno = errno;
        close(fd);
        errno = save_errno;
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not fsync file \"%s\": %m", tmppath)));
    }
    pgstat_report_wait_end();

    if (close(fd) != 0)
        ereport(ERROR, (errcode_for_file_access(),
                       errmsg("could not close file \"%s\": %m", tmppath)));

    // Step 5: Atomically install the file in its final location
    installed_segno = logsegno;
    max_segno = logsegno + CheckPointSegments;

    if (InstallXLogFileSegment(&installed_segno, tmppath, true, max_segno, logtli)) {
        *added = true;
        elog(DEBUG2, "done creating and filling new WAL file");
    } else {
        // Installation failed or not needed, clean up
        unlink(tmppath);
        elog(DEBUG2, "abandoned new WAL file");
    }

    return -1;  // Success: caller should open the path directly
}
```

Key simplifications made:
- Reorganized into clear sequential steps with section comments
- Streamlined error handling while preserving all critical checks
- Consolidated variable declarations and initialization
- Clarified the two-phase creation strategy (existing vs new file)
- Simplified the file content initialization logic
- Maintained atomic file installation and cleanup semantics
- Preserved all essential functionality including wait event reporting
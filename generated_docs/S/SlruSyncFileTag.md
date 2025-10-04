# SlruSyncFileTag

## Location
[src/backend/access/transam/slru.c:1828-1849](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L1828-L1849)

## Overview
A common implementation function that performs file synchronization (fsync) operations for SLRU (Simple Least Recently Used) segments in PostgreSQL's transaction log system.

## Definition

```c
int
SlruSyncFileTag(SlruCtl ctl, const FileTag *ftag, char *path)
```
## Detailed Description
SlruSyncFileTag is a shared implementation function used by individual SLRU subsystems (such as clog, commit timestamp, and multixact) to perform file synchronization operations. The function takes an SLRU control structure and a file tag, constructs the appropriate file path, opens the file, and performs an fsync operation to ensure data is written to disk. This common implementation allows different SLRU subsystems to delegate their sync operations while providing their specific SlruCtl context.

The function opens the file in read-write mode with binary flags, performs the fsync operation while reporting wait events for monitoring purposes, and properly handles error conditions by preserving errno values.

## Parameters / Member Variables
- `ctl`: SlruCtl structure containing the SLRU control information needed to build the file path and handle the sync operation
- `*ftag`: FileTag structure containing the segment number and other file identification information
- `*path`: Character buffer where the constructed file path will be stored
## Dependencies
- Functions called/Symbols referenced:
  - [SlruFileName](SlruFileName.md) (constructs the SLRU file name)
  - [OpenTransientFile](../O/OpenTransientFile.md) (opens the file for syncing)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md) (reports wait event start)
  - [pg_fsync](../p/pg_fsync.md) (performs the actual file sync)
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md) (reports wait event end)
  - [CloseTransientFile](../C/CloseTransientFile.md) (closes the opened file)
  - PG_BINARY (file open mode flag)
- Called from (representative examples):
  - [clogsyncfiletag](../c/clogsyncfiletag.md) (CLOG sync handler)
  - [committssyncfiletag](../c/committssyncfiletag.md) (commit timestamp sync handler)
  - [multixactoffsetssyncfiletag](../m/multixactoffsetssyncfiletag.md) (multixact offset sync handler)
  - [multixactmemberssyncfiletag](../m/multixactmemberssyncfiletag.md) (multixact member sync handler)
  - [test_slru_page_sync](../t/test_slru_page_sync.md) (test module function)

## Notes and Other Information
- This function serves as a common sync implementation that individual SLRU subsystems can use by providing their specific SlruCtl structure
- The function properly handles error conditions by preserving errno across the CloseTransientFile call
- Wait events are reported during the sync operation to enable monitoring and performance analysis
- Returns -1 on file open failure, or the result of pg_fsync on success
- Located in src/backend/access/transam/slru.c at lines 1828-1849

## Simplified Source

```c
int SlruSyncFileTag(SlruCtl ctl, const FileTag *ftag, char *path)
{
    // Build the file path using SLRU control and segment number
    SlruFileName(ctl, path, ftag->segno);

    // Open the file for read-write access
    int fd = OpenTransientFile(path, O_RDWR | PG_BINARY);
    if (fd < 0)
        return -1;

    // Perform the sync operation with wait event reporting
    pgstat_report_wait_start(WAIT_EVENT_SLRU_FLUSH_SYNC);
    int result = pg_fsync(fd);
    pgstat_report_wait_end();

    // Preserve errno across file close
    int save_errno = errno;
    CloseTransientFile(fd);
    errno = save_errno;

    return result;
}
```
# SlruPhysicalWritePage

## Location
[src/backend/access/transam/slru.c:873-1044](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L873-L1044)

## Overview
Low-level function that performs physical disk I/O to write SLRU pages from shared memory buffers to persistent storage, with comprehensive WAL synchronization and error handling.

## Definition

```c
static bool
SlruPhysicalWritePage(SlruCtl ctl, int64 pageno, int slotno, SlruWriteAll fdata)
```
## Detailed Description
SlruPhysicalWritePage is the core low-level function responsible for writing SLRU pages from shared memory buffers to disk storage. It implements PostgreSQL's write-ahead logging (WAL) protocol, sophisticated error handling, and optimization for batch write operations.

The function performs several critical operations:

1. **WAL Protocol Enforcement**: Ensures WAL records are flushed before data pages (write-WAL-before-data rule) by finding the maximum LSN for the page and calling XLogFlush
2. **Batch Write Optimization**: Reuses file descriptors during SimpleLruWriteAll operations to improve performance
3. **File Creation Handling**: Creates SLRU segment files as needed, handling the case where pages are written out-of-order
4. **Atomic I/O Operations**: Uses pg_pwrite for positioned writes that don't affect file position
5. **Sync Request Management**: Queues background sync requests or performs synchronous fsync when the sync request queue is full
6. **Resource Management**: Properly manages file descriptors and handles both standalone and batch write scenarios

The function implements PostgreSQL's durability guarantees while providing optimal performance through batching and background synchronization.

## Parameters / Member Variables
- `ctl`: SlruCtl control structure containing SLRU configuration and shared memory state
- `pageno`: 64-bit logical page number identifying which page to write
- `slotno`: Integer buffer slot number containing the page data to write
- `fdata`: SlruWriteAll structure for batch operations (NULL for standalone writes) containing open file descriptors and metadata
## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_count_slru_page_written](../p/pgstat_count_slru_page_written.md)
  - XLogRecPtrIsInvalid
  - [XLogFlush](../X/XLogFlush.md)
  - [SlruFileName](SlruFileName.md)
  - [OpenTransientFile](../O/OpenTransientFile.md)
  - [pg_pwrite](../p/pg_pwrite.md)
  - [RegisterSyncRequest](../R/RegisterSyncRequest.md)
  - [pg_fsync](../p/pg_fsync.md)
  - [CloseTransientFile](../C/CloseTransientFile.md)
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md)/end
  - START_CRIT_SECTION/END_CRIT_SECTION
- Called from (representative examples):
  - [SlruInternalWritePage](SlruInternalWritePage.md)

## Notes and Other Information
- Never calls ereport(ERROR) directly - returns false and saves error info for later reporting to allow caller cleanup
- Implements write-WAL-before-data rule by determining maximum LSN across all LSN groups for the page
- Creates SLRU segment files on demand, handling recovery scenarios where truncated segments need recreation  
- Optimizes batch writes by reusing file descriptors across multiple page writes in SimpleLruWriteAll
- Uses critical sections around XLogFlush to ensure PANIC on failure rather than ERROR
- Integrates with PostgreSQL's background sync system for optimal I/O performance
- Updates statistics counters for monitoring SLRU write activity
- Handles disk space exhaustion by setting errno to ENOSPC when write doesn't set errno
- Critical component of PostgreSQL's transaction durability infrastructure

## Simplified Source

```c
static bool SlruPhysicalWritePage(SlruCtl ctl, int64 pageno, int slotno, SlruWriteAll fdata) {
    SlruShared shared = ctl->shared;
    int64 segno = pageno / SLRU_PAGES_PER_SEGMENT;
    int rpageno = pageno % SLRU_PAGES_PER_SEGMENT;
    off_t offset = rpageno * BLCKSZ;
    char path[MAXPGPATH];
    int fd = -1;

    // Update statistics
    pgstat_count_slru_page_written(shared->slru_stats_idx);

    // Enforce WAL-before-data rule - find maximum LSN and flush WAL
    if (shared->group_lsn != NULL) {
        XLogRecPtr max_lsn = InvalidXLogRecPtr;
        int lsnindex = slotno * shared->lsn_groups_per_page;

        // Find maximum LSN across all groups for this page
        for (int i = 0; i < shared->lsn_groups_per_page; i++) {
            if (max_lsn < shared->group_lsn[lsnindex + i])
                max_lsn = shared->group_lsn[lsnindex + i];
        }

        // Flush WAL up to max LSN before writing data
        if (!XLogRecPtrIsInvalid(max_lsn)) {
            START_CRIT_SECTION();
            XLogFlush(max_lsn);
            END_CRIT_SECTION();
        }
    }

    // Check if file is already open during batch operation
    if (fdata) {
        for (int i = 0; i < fdata->num_files; i++) {
            if (fdata->segno[i] == segno) {
                fd = fdata->fd[i];
                break;
            }
        }
    }

    // Open file if not already open
    if (fd < 0) {
        SlruFileName(ctl, path, segno);
        fd = OpenTransientFile(path, O_RDWR | O_CREAT | PG_BINARY);
        if (fd < 0) {
            slru_errcause = SLRU_OPEN_FAILED;
            slru_errno = errno;
            return false;
        }

        // Cache fd for batch operations if possible
        if (fdata && fdata->num_files < MAX_WRITEALL_BUFFERS) {
            fdata->fd[fdata->num_files] = fd;
            fdata->segno[fdata->num_files] = segno;
            fdata->num_files++;
        }
    }

    // Write the page data
    errno = 0;
    pgstat_report_wait_start(WAIT_EVENT_SLRU_WRITE);
    if (pg_pwrite(fd, shared->page_buffer[slotno], BLCKSZ, offset) != BLCKSZ) {
        pgstat_report_wait_end();
        if (errno == 0)
            errno = ENOSPC;
        slru_errcause = SLRU_WRITE_FAILED;
        slru_errno = errno;
        if (!fdata)
            CloseTransientFile(fd);
        return false;
    }
    pgstat_report_wait_end();

    // Queue sync request or sync immediately if queue full
    if (ctl->sync_handler != SYNC_HANDLER_NONE) {
        FileTag tag;
        INIT_SLRUFILETAG(tag, ctl->sync_handler, segno);

        if (!RegisterSyncRequest(&tag, SYNC_REQUEST, false)) {
            // Sync queue full, do immediate sync
            pgstat_report_wait_start(WAIT_EVENT_SLRU_SYNC);
            if (pg_fsync(fd) != 0) {
                pgstat_report_wait_end();
                slru_errcause = SLRU_FSYNC_FAILED;
                slru_errno = errno;
                CloseTransientFile(fd);
                return false;
            }
            pgstat_report_wait_end();
        }
    }

    // Close file if not part of batch operation
    if (!fdata) {
        if (CloseTransientFile(fd) != 0) {
            slru_errcause = SLRU_CLOSE_FAILED;
            slru_errno = errno;
            return false;
        }
    }

    return true;
}
```
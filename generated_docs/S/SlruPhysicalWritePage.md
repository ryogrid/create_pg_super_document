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
- : SlruCtl control structure containing SLRU configuration and shared memory state
- : 64-bit logical page number identifying which page to write
- : Integer buffer slot number containing the page data to write
- : SlruWriteAll structure for batch operations (NULL for standalone writes) containing open file descriptors and metadata

## Dependencies
- Functions called/Symbols referenced:
  - [pgstat_count_slru_page_written](../p/pgstat_count_slru_page_written.md)
  - XLogRecPtrIsInvalid
  - [XLogFlush](../X/XLogFlush.md)
  - [SlruFileName](SlruFileName.md)
  - OpenTransientFile
  - pg_pwrite
  - [RegisterSyncRequest](../R/RegisterSyncRequest.md)
  - pg_fsync
  - CloseTransientFile
  - pgstat_report_wait_start/end
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
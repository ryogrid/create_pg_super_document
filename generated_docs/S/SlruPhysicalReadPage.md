# SlruPhysicalReadPage

## Location
[src/backend/access/transam/slru.c:801-872](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/access/transam/slru.c#L801-L872)

## Overview
Low-level function that performs the physical disk I/O operation to read a specific SLRU page from storage into a shared memory buffer slot.

## Definition

```c
static bool
SlruPhysicalReadPage(SlruCtl ctl, int64 pageno, int slotno)
```
## Detailed Description
SlruPhysicalReadPage is a critical low-level function that handles the actual disk I/O for reading SLRU pages from persistent storage into shared memory buffers. The function implements sophisticated error handling and recovery logic to ensure system stability even under adverse conditions.

Key functionality includes:
1. **File Path Resolution**: Converts logical page numbers to physical file paths using segment-based organization
2. **Recovery-Aware File Handling**: During recovery, gracefully handles missing files by returning zero-filled pages instead of failing
3. **Atomic I/O Operations**: Uses pg_pread for positioned reads without affecting file position, enabling concurrent access
4. **Wait Event Reporting**: Integrates with PostgreSQL's wait event system for monitoring and diagnostics
5. **Comprehensive Error Handling**: Returns false on errors and saves detailed error information for later reporting
6. **Resource Management**: Properly manages transient file descriptors to avoid resource leaks

The function is designed to never call ereport(ERROR) directly since callers may have modified shared memory state that must be cleaned up before error reporting.

## Parameters / Member Variables
- : SlruCtl control structure containing SLRU configuration and shared memory pointers
- : 64-bit logical page number to read from disk
- : Integer identifying the shared memory buffer slot to read data into

## Dependencies
- Functions called/Symbols referenced:
  - [SlruFileName](SlruFileName.md)
  - OpenTransientFile
  - pg_pread
  - pgstat_report_wait_start
  - pgstat_report_wait_end
  - CloseTransientFile
  - MemSet
  - SLRU_PAGES_PER_SEGMENT
  - PG_BINARY
- Called from (representative examples):
  - [SimpleLruReadPage](SimpleLruReadPage.md)

## Notes and Other Information
- Returns boolean success/failure status rather than using ereport(ERROR) to allow caller cleanup
- During recovery (InRecovery), missing files are treated as containing all zeros - this handles cases where truncated commit log segments are referenced
- Uses pg_pread for atomic positioned reads that don't interfere with concurrent file operations
- Integrates with PostgreSQL's wait event monitoring system (WAIT_EVENT_SLRU_READ)
- Manages transient file descriptors efficiently - files are not kept open between operations
- Critical path for all SLRU read operations in PostgreSQL's transaction status subsystems
- Error information is stored in static variables for later reporting by SlruReportIOError
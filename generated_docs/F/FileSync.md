# FileSync

## Location
[src/backend/storage/file/fd.c:2294-2320](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2294-L2320)

## Overview
FileSync synchronizes a virtual file descriptor to persistent storage, ensuring that all buffered data is written to disk.

## Definition

```c
int
FileSync(File file, uint32 wait_event_info)
```
## Detailed Description
FileSync performs a synchronous write operation on a virtual file descriptor, ensuring data durability by forcing all buffered writes to be committed to persistent storage. The function validates the file descriptor, accesses the underlying system file, and calls the PostgreSQL fsync wrapper with proper wait event reporting for monitoring purposes. This is a critical operation for ensuring data consistency and durability in PostgreSQL's storage layer.

## Parameters / Member Variables
- `file`: Virtual file descriptor representing the file to be synchronized
- `wait_event_info`: Event information used for wait event reporting during the sync operation

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the virtual file descriptor
  - [FileAccess](FileAccess.md): Ensures the file is accessible and handles VFD management
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md): Reports the start of a wait event for monitoring
  - [pg_fsync](../p/pg_fsync.md): PostgreSQL's fsync wrapper that performs the actual sync operation
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md): Reports the end of the wait event
  - DO_DB: Debug logging macro
- Called from (representative examples):
  - [logical_end_heap_rewrite](../l/logical_end_heap_rewrite.md): During heap rewrite completion
  - [bbsink_server_end_archive](../b/bbsink_server_end_archive.md): At the end of base backup archiving
  - [mdimmedsync](../m/mdimmedsync.md): For immediate synchronization in the MD storage manager
  - [register_dirty_segment](../r/register_dirty_segment.md): When registering dirty segments for sync
  - [mdsyncfiletag](../m/mdsyncfiletag.md): During tagged file synchronization operations

## Notes and Other Information
- Returns 0 on success, or a negative error code on failure
- The function includes debug logging when DO_DB is enabled
- Wait event reporting allows PostgreSQL to track sync operations for performance monitoring
- This is part of PostgreSQL's Virtual File Descriptor (VFD) system that manages file handles efficiently
- Critical for ensuring ACID properties, particularly durability
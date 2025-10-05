# FileWriteback

## Location
[src/backend/storage/file/fd.c:2107-2132](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2107-L2132)

## Overview
FileWriteback forces dirty data in a specified file range to be written back to storage, optimizing I/O performance by controlling when data is flushed.

## Definition

```c
void
FileWriteback(File file, off_t offset, off_t nbytes, uint32 wait_event_info)
```
## Detailed Description
FileWriteback implements forced writeback of dirty pages for a specific range of a file. This function is crucial for PostgreSQL's buffer management and checkpoint operations, allowing the system to control when modified data is actually written to persistent storage.

The function performs the following operations:
1. **File Validation**: Ensures the file descriptor is valid
2. **Range Validation**: Returns early if nbytes is zero or negative
3. **Direct I/O Check**: Skips writeback for files opened with PG_O_DIRECT flag since they bypass OS buffers
4. **File Access**: Ensures the file is accessible and opens if needed
5. **Data Flushing**: Uses pg_flush_data() to force the OS to write dirty pages in the specified range to storage
6. **Wait Event Reporting**: Reports wait events for performance monitoring

This function is particularly important for checkpoint operations and buffer management, where PostgreSQL needs to ensure data consistency by controlling when dirty pages are written to disk.

## Parameters / Member Variables
- `file`: The virtual file descriptor to perform writeback on
- `offset`: Starting byte offset within the file for the writeback operation
- `nbytes`: Number of bytes to write back from the offset
- `wait_event_info`: Wait event identifier for PostgreSQL's wait event monitoring system
## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the file descriptor
  - [FileAccess](FileAccess.md): Ensures file is accessible and opens if needed
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md): Reports start of wait event for monitoring
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md): Reports end of wait event
  - [pg_flush_data](../p/pg_flush_data.md): Platform-specific function to flush data to storage
  - PG_O_DIRECT: Flag indicating direct I/O mode
- Called from (representative examples):
  - [mdwriteback](../m/mdwriteback.md): Magnetic disk storage manager writeback operations

## Notes and Other Information
- The function is a no-op for files opened with PG_O_DIRECT since such files bypass OS page cache
- Essential for PostgreSQL's checkpoint mechanism and buffer management strategy
- Helps prevent large I/O spikes by spreading write operations over time
- The function may block until the specified data range is written to storage
- Used in background writer and checkpointer processes to maintain consistent performance
- Platform-specific implementation via pg_flush_data() handles differences between operating systems

## Simplified Source

```c
void FileWriteback(File file, off_t offset, off_t nbytes, uint32 wait_event_info) {
    Assert(FileIsValid(file));

    // Skip if no data to write back
    if (nbytes <= 0)
        return;

    // Skip writeback for direct I/O files (they bypass OS buffers)
    if (VfdCache[file].fileFlags & PG_O_DIRECT)
        return;

    // Ensure file is accessible
    int returnCode = FileAccess(file);
    if (returnCode < 0)
        return;

    // Force dirty pages to storage with wait event tracking
    pgstat_report_wait_start(wait_event_info);
    pg_flush_data(VfdCache[file].fd, offset, nbytes);
    pgstat_report_wait_end();
}
```
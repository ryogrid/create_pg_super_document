# FilePrefetch

## Location
[src/backend/storage/file/fd.c:2075-2106](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L2075-L2106)

## Overview
FilePrefetch initiates asynchronous read-ahead operations on a file range to improve I/O performance by preloading data into system buffers.

## Definition

```c
int
FilePrefetch(File file, off_t offset, off_t amount, uint32 wait_event_info)
```
## Detailed Description
FilePrefetch implements asynchronous I/O prefetching for PostgreSQL's virtual file descriptor system. The function uses the POSIX  system call with the  flag to hint to the operating system that the specified range of the file will be needed soon. This allows the OS to proactively read the data into system buffers, potentially reducing future I/O wait times.

The implementation includes:
1. **File Validation**: Ensures the file descriptor is valid
2. **File Access**: Calls FileAccess to ensure the file is open and available
3. **Wait Event Reporting**: Reports wait events for monitoring and performance analysis
4. **Retry Logic**: Handles EINTR interruptions by retrying the operation
5. **Platform Compatibility**: Provides a no-op implementation on systems without posix_fadvise support

The function is designed to be non-blocking - it returns immediately after issuing the prefetch hint, allowing the calling code to continue while the OS performs the actual I/O in the background.

## Parameters / Member Variables
- `file`: The virtual file descriptor to prefetch from
- `offset`: Starting byte offset within the file for the prefetch operation
- `amount`: Number of bytes to prefetch from the offset
- `wait_event_info`: Wait event identifier for PostgreSQL's wait event monitoring system
## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the file descriptor
  - [FileAccess](FileAccess.md): Ensures file is accessible and opens if needed
  - [pgstat_report_wait_start](../p/pgstat_report_wait_start.md): Reports start of wait event for monitoring
  - [pgstat_report_wait_end](../p/pgstat_report_wait_end.md): Reports end of wait event
  - posix_fadvise: POSIX system call for file access pattern hints
- Called from (representative examples):
  - [mdprefetch](../m/mdprefetch.md): Magnetic disk storage manager prefetch operations

## Notes and Other Information
- The function only works on systems with USE_POSIX_FADVISE and POSIX_FADV_WILLNEED support
- On unsupported platforms, the function returns 0 (success) but performs no actual prefetching
- The API is designed for advisory prefetching and is not suitable for mandatory asynchronous I/O APIs like libaio
- Return value follows posix_fadvise conventions: 0 on success, error code on failure
- The function includes wait event reporting for PostgreSQL's performance monitoring infrastructure
- Prefetching can significantly improve sequential read performance by overlapping I/O with computation

## Simplified Source

```c
int FilePrefetch(File file, off_t offset, off_t amount, uint32 wait_event_info) {
#if defined(USE_POSIX_FADVISE) && defined(POSIX_FADV_WILLNEED)
    Assert(FileIsValid(file));

    // Ensure file is accessible and open
    int returnCode = FileAccess(file);
    if (returnCode < 0)
        return returnCode;

    // Issue prefetch hint with retry on interruption
retry:
    pgstat_report_wait_start(wait_event_info);
    returnCode = posix_fadvise(VfdCache[file].fd, offset, amount, POSIX_FADV_WILLNEED);
    pgstat_report_wait_end();

    if (returnCode == EINTR)
        goto retry;  // Handle signal interruption

    return returnCode;  // 0 on success, error code on failure
#else
    // No-op on platforms without posix_fadvise support
    Assert(FileIsValid(file));
    return 0;
#endif
}
```
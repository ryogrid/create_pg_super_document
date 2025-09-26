# FileWriteV

## Location
src/backend/storage/file/fd.c: 2189 - 2293

## Overview
FileWriteV performs vectored (gather) I/O write operations on a file, allowing multiple buffers to be written in a single system call with integrated temporary file size tracking.

## Definition


## Detailed Description
FileWriteV implements vectored I/O writing for PostgreSQL's virtual file descriptor system. This function allows writing data from multiple non-contiguous memory buffers to a file in a single system call, which is significantly more efficient than multiple separate write operations.

Key features include:
1. **File Validation**: Ensures the file descriptor is valid
2. **File Access**: Calls FileAccess to ensure the file is open and available
3. **Temporary File Limit Enforcement**: Checks if the write would exceed temp_file_limit for temporary files and throws an error if so
4. **Vectored Writing**: Uses pg_pwritev() to perform the actual vectored write operation at the specified offset
5. **File Size Tracking**: Maintains accurate file size and temporary_files_size counters for temporary files
6. **Wait Event Reporting**: Reports wait events for PostgreSQL's performance monitoring
7. **Error Handling**: Includes platform-specific error handling and retry logic with special Windows support

The function includes sophisticated temporary file management, automatically tracking file sizes and enforcing PostgreSQL's temporary file limits to prevent runaway temporary file growth.

## Parameters / Member Variables
- : The virtual file descriptor to write to
- : Array of iovec structures specifying the buffers to write from
- : Number of iovec structures in the array
- : Starting byte offset within the file for the write operation
- : Wait event identifier for PostgreSQL's wait event monitoring system

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the file descriptor
  - FileAccess: Ensures file is accessible and opens if needed
  - pgstat_report_wait_start: Reports start of wait event for monitoring
  - pgstat_report_wait_end: Reports end of wait event
  - pg_pwritev: Platform-specific vectored write function
  - pg_usleep: Windows-specific sleep function for retry handling
  - _dosmaperr: Windows error mapping function
  - FD_TEMP_FILE_LIMIT: Flag indicating temporary file with size limits
- Called from (representative examples):
  - mdwritev: Magnetic disk storage manager vectored write operations
  - FileWrite: Single buffer write operation (wrapper function)

## Notes and Other Information
- Enforces PostgreSQL's temp_file_limit configuration parameter for temporary files
- Automatically updates file size tracking for temporary files to maintain accurate usage statistics
- Sets errno to ENOSPC on successful writes to help callers detect short writes indicating disk space issues
- More efficient than multiple single-buffer writes when writing from scattered memory locations
- Essential for high-performance I/O operations in PostgreSQL's buffer management and temporary file systems
- Includes retry logic for EINTR (interrupted system call) errors and Windows-specific resource exhaustion handling
- The iovec array allows specifying multiple buffers of different sizes in a single operation
- Throws ERROR (not just returning -1) when temporary file limits are exceeded, which is a modularity violation noted in the code but necessary for proper error reporting
# FileReadV

## Location
src/backend/storage/file/fd.c: 2133 - 2188

## Overview
FileReadV performs vectored (scatter-gather) I/O read operations on a file, allowing multiple buffers to be filled in a single system call for improved performance.

## Definition


## Detailed Description
FileReadV implements vectored I/O reading for PostgreSQL's virtual file descriptor system. This function allows reading data from a file into multiple non-contiguous memory buffers in a single system call, which can be significantly more efficient than multiple separate read operations.

The function provides:
1. **File Validation**: Ensures the file descriptor is valid
2. **File Access**: Calls FileAccess to ensure the file is open and available
3. **Vectored Reading**: Uses pg_preadv() to perform the actual vectored read operation at the specified offset
4. **Wait Event Reporting**: Reports wait events for PostgreSQL's performance monitoring
5. **Error Handling**: Includes platform-specific error handling and retry logic
6. **Windows Compatibility**: Special handling for Windows kernel buffer exhaustion issues

The vectored I/O approach is particularly beneficial when reading scattered data pages or when the application needs to read into multiple separate buffers that are not physically contiguous in memory.

## Parameters / Member Variables
- : The virtual file descriptor to read from
- : Array of iovec structures specifying the buffers to read into
- : Number of iovec structures in the array
- : Starting byte offset within the file for the read operation
- : Wait event identifier for PostgreSQL's wait event monitoring system

## Dependencies
- Functions called/Symbols referenced:
  - FileIsValid: Validates the file descriptor
  - FileAccess: Ensures file is accessible and opens if needed
  - pgstat_report_wait_start: Reports start of wait event for monitoring
  - pgstat_report_wait_end: Reports end of wait event
  - pg_preadv: Platform-specific vectored read function
  - pg_usleep: Windows-specific sleep function for retry handling
  - _dosmaperr: Windows error mapping function
- Called from (representative examples):
  - mdreadv: Magnetic disk storage manager vectored read operations
  - FileRead: Single buffer read operation (wrapper function)

## Notes and Other Information
- The function supports both Unix and Windows platforms with appropriate error handling for each
- On Windows, includes special retry logic for 'Insufficient system resources' errors caused by kernel buffer exhaustion
- Returns the number of bytes actually read, or -1 on error following standard Unix conventions
- More efficient than multiple single-buffer reads when reading into scattered memory locations
- Essential for high-performance I/O operations in PostgreSQL's buffer management system
- The iovec array allows specifying multiple buffers of different sizes in a single operation
- Includes retry logic for EINTR (interrupted system call) errors on Unix systems
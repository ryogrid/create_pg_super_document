# FileRead

## Location
src/include/storage/fd.h: 196 - 207

## Overview
A convenience wrapper function that performs a single-buffer read operation from a PostgreSQL virtual file descriptor using vectored I/O internally.

## Definition


## Detailed Description
FileRead is an inline function that provides a simplified interface for reading data from a PostgreSQL virtual file. It wraps the more general FileReadV function by converting a single buffer read request into a vectored I/O operation with a single iovec structure. This function is part of PostgreSQL's virtual file descriptor (VFD) system, which manages file handles efficiently by automatically closing and reopening files as needed to stay within system limits.

The function constructs an iovec structure from the provided buffer and amount parameters, then delegates the actual read operation to FileReadV, which handles the low-level file access, error handling, and wait event reporting.

## Parameters / Member Variables
- : PostgreSQL virtual file descriptor (File type) identifying the file to read from
- : Pointer to the memory buffer where read data will be stored
- : Number of bytes to read from the file
- : File offset position from which to start reading
- : Wait event identifier for PostgreSQL's wait event monitoring system

## Dependencies
- Functions called/Symbols referenced:
  - FileReadV (the underlying vectored read function)
  - iovec (POSIX structure for vectored I/O operations)
  - File (PostgreSQL virtual file descriptor type)
  - ssize_t (POSIX signed size type)

- Called from (representative examples):
  - ReadWalSummary (in src/backend/backup/walsummary.c:278)
  - ReorderBufferRestoreChanges (in src/backend/replication/logical/reorderbuffer.c:4460, 4491)
  - BufFileLoadBuffer (in src/backend/storage/file/buffile.c:460)

## Notes and Other Information
- This is an inline function defined in src/include/storage/fd.h, making it efficient for frequent use
- The function is a thin wrapper that simplifies the interface for single-buffer reads while leveraging the robust error handling and retry logic in FileReadV
- Uses PostgreSQL's wait event reporting system to track I/O operations for monitoring and debugging
- Returns the number of bytes actually read, or -1 on error following standard POSIX conventions
- Part of PostgreSQL's VFD system which provides automatic file handle management and efficient resource usage
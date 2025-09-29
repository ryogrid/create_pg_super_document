# FileWrite

## Location
[src/include/storage/fd.h:208-219](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/include/storage/fd.h#L208-L219)

## Overview
A convenience wrapper function that performs a single-buffer write operation to a PostgreSQL virtual file descriptor using vectored I/O internally.

## Definition
```c
static inline ssize_t FileWrite(File file, const void *buffer, size_t amount, off_t offset, uint32 wait_event_info)
```

## Detailed Description
FileWrite is an inline function that provides a simplified interface for writing data to a PostgreSQL virtual file. Similar to FileRead, it wraps the more general FileWriteV function by converting a single buffer write request into a vectored I/O operation with a single iovec structure. This function is part of PostgreSQL's virtual file descriptor (VFD) system, which manages file handles efficiently.

The function constructs an iovec structure from the provided buffer and amount parameters, using the unconstify macro to safely cast away the const qualifier from the buffer pointer (as required by the iovec structure), then delegates the actual write operation to FileWriteV, which handles low-level file access, temporary file limits, error handling, and wait event reporting.

## Parameters / Member Variables
- `file`: PostgreSQL virtual file descriptor (File type) identifying the file to write to
- `buffer`: Pointer to the memory buffer containing data to be written (const-qualified)
- `amount`: Number of bytes to write to the file
- `offset`: File offset position where writing should start
- `wait_event_info`: Wait event identifier for PostgreSQL's wait event monitoring system

## Dependencies
- Functions called/Symbols referenced:
  - [FileWriteV](FileWriteV.md) (the underlying vectored write function)
  - unconstify (macro to safely cast away const qualifier)
  - [iovec](../i/iovec.md) (POSIX structure for vectored I/O operations)
  - File (PostgreSQL virtual file descriptor type)
  - ssize_t (POSIX signed size type)

- Called from (representative examples):
  - [logical_heap_rewrite_flush_mappings](../l/logical_heap_rewrite_flush_mappings.md) (in src/backend/access/heap/rewriteheap.c:880)
  - [bbsink_server_archive_contents](../b/bbsink_server_archive_contents.md) (in src/backend/backup/basebackup_server.c:165)
  - [bbsink_server_manifest_contents](../b/bbsink_server_manifest_contents.md) (in src/backend/backup/basebackup_server.c:258)
  - [WriteWalSummary](../W/WriteWalSummary.md) (in src/backend/backup/walsummary.c:299)
  - [BufFileDumpBuffer](../B/BufFileDumpBuffer.md) (in src/backend/storage/file/buffile.c:537)
  - [mdextend](../m/mdextend.md) (in src/backend/storage/smgr/md.c:495)

## Notes and Other Information
- This is an inline function defined in src/include/storage/fd.h, making it efficient for frequent use
- The function is a thin wrapper that simplifies the interface for single-buffer writes while leveraging the comprehensive functionality in FileWriteV
- Uses the unconstify macro to handle the const-correctness mismatch between the function's const buffer parameter and iovec's non-const iov_base field
- [FileWriteV](FileWriteV.md) provides sophisticated features including temporary file size limit enforcement, automatic file size tracking for temp files, and robust error handling with retry logic
- Uses PostgreSQL's wait event reporting system to track I/O operations for monitoring and debugging
- Returns the number of bytes actually written, or -1 on error following standard POSIX conventions
- Part of PostgreSQL's VFD system which provides automatic file handle management and efficient resource usage
- The underlying FileWriteV function enforces temp_file_limit for temporary files and maintains accurate file size tracking

## Simplified Source

```c
static inline ssize_t FileWrite(File file, const void *buffer, size_t amount,
                               off_t offset, uint32 wait_event_info) {
    // Convert single buffer to vectored I/O structure
    struct iovec iov = {
        .iov_base = unconstify(void *, buffer),
        .iov_len = amount
    };

    // Delegate to vectored write function
    return FileWriteV(file, &iov, 1, offset, wait_event_info);
}
```
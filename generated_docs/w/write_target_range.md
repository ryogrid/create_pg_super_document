# write_target_range

## Location
[src/bin/pg_rewind/file_ops.c:88-129](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/bin/pg_rewind/file_ops.c#L88-L129)

## Overview
Writes a buffer of data to a specific offset range in the currently open target file, with progress reporting and dry-run support.

## Definition

```c
void
write_target_range(char *buf, off_t begin, size_t size)
```
## Detailed Description
This function writes a specified buffer to a target file at a given offset. It performs robust error handling with retry logic for partial writes, ensuring all data is written successfully. The function updates global progress tracking and respects the dry_run mode. It seeks to the specified position in the file and then writes the data in a loop to handle cases where the write system call doesn't write all requested bytes in a single operation. The function maintains the file open after writing to allow for subsequent operations.

## Parameters / Member Variables
- `*buf`: Pointer to the buffer containing data to write
- `begin`: File offset position where writing should start (off_t type)
- `size`: Number of bytes to write from the buffer
## Dependencies
- Functions called/Symbols referenced:
  - lseek (system call)
  - write (system call) 
  - [progress_report](../p/progress_report.md)
  - [pg_fatal](../p/pg_fatal.md)
- Global variables used:
  - fetch_done (progress tracking counter)
  - dry_run (configuration flag)
  - dstfd (static file descriptor)
  - dstpath (static path buffer for error reporting)
- Called from (representative examples):
  - [process_queued_fetch_requests](../p/process_queued_fetch_requests.md)
  - [local_queue_fetch_file](../l/local_queue_fetch_file.md)
  - [local_queue_fetch_range](../l/local_queue_fetch_range.md)
  - [createBackupLabel](../c/createBackupLabel.md)

## Notes and Other Information
- Part of pg_rewind utility's file operations module (src/bin/pg_rewind/file_ops.c)
- Implements robust write logic with partial write handling
- Updates progress tracking for user feedback during long operations
- Uses ENOSPC (no space left on device) as default error when write() fails without setting errno
- Critical for applying file differences during PostgreSQL data directory synchronization
- Maintains file position for subsequent writes without reopening
- Essential component of pg_rewind's block-level file copying mechanism
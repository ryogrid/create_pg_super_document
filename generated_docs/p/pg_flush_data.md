# pg_flush_data

## Location
[src/backend/storage/file/fd.c:522-699](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/backend/storage/file/fd.c#L522-L699)

## Overview
A PostgreSQL function that advises the operating system to flush dirty data from memory to storage, providing platform-specific optimization for write-back operations.

## Definition
```c
void pg_flush_data(int fd, off_t offset, off_t nbytes)
```

## Detailed Description
The pg_flush_data function provides a platform-agnostic interface to hint to the operating system that dirty data in a specified range should be written to storage. This function is primarily used as an optimization to reduce the impact of later fsync()/fdatasync() calls by initiating writeback operations early. The function implements multiple platform-specific approaches, compiled conditionally based on system capabilities:

1. **Linux sync_file_range()**: The preferred method that starts writeback without waiting for completion
2. **mmap/msync approach**: Used on Unix systems with MS_ASYNC support, mapping the file region and using msync() to trigger writeback  
3. **posix_fadvise()**: Fallback method using POSIX_FADV_DONTNEED, though it has the side effect of discarding clean cached blocks

The function respects the enableFsync setting and returns early if fsync operations are disabled.

## Parameters / Member Variables
- `fd`: File descriptor of the file to flush
- `offset`: Starting byte offset of the range to flush (0 for entire file when combined with nbytes=0)
- `nbytes`: Number of bytes to flush from offset (0 with offset=0 means flush entire file)

## Dependencies
- Functions called/Symbols referenced:
  - sync_file_range (Linux)
  - lseek 
  - mmap/msync/munmap (Unix)
  - posix_fadvise (POSIX)
  - data_sync_elevel
  - ereport
  - errcode_for_file_access
- Called from (representative examples):
  - FileWriteback
  - pre_sync_fname

## Notes and Other Information
- Does nothing if enableFsync is disabled
- Implements retry logic for EINTR interruptions on sync_file_range
- Handles platform incompatibilities gracefully (e.g., ENOSYS on WSL)
- Aligns mmap operations to page boundaries for compatibility
- Uses different error levels based on the severity and recoverability of failures
- The function is a performance optimization hint - failures are generally non-fatal
- Part of PostgreSQL's advanced I/O management system for better write performance
# pg_pwrite_zeros

## Location
src/common/file_utils.c: 687 - 728

## Overview
Efficiently writes a specified amount of zero-filled data to a file at a given offset using vectored I/O operations.

## Definition
```c
ssize_t pg_pwrite_zeros(int fd, size_t size, off_t offset)
```

## Detailed Description
pg_pwrite_zeros provides an efficient way to write large amounts of zero data to files by utilizing a static zero-filled buffer and vectored I/O operations. Instead of allocating and zeroing memory dynamically, the function uses a single statically allocated BLCKSZ-sized zero buffer that is reused across multiple iovec entries.

The function works by:
1. Using a static PGIOAlignedBlock buffer filled with zeros
2. Setting up iovec arrays that all point to the same zero buffer  
3. Using pg_pwritev_with_retry() to perform the actual vectored write operations
4. Looping until all requested data has been written

This approach is memory-efficient since it only requires a single block-sized buffer regardless of how much zero data needs to be written. The vectored I/O allows the kernel to optimize the write operations.

Key behaviors:
- Uses a single static zero buffer shared across all iovec entries
- Writes data in chunks up to PG_IOV_MAX * BLCKSZ per system call
- Handles partial writes through pg_pwritev_with_retry()
- Returns total bytes written on success, negative value on error
- Maintains file offset tracking across multiple write operations

## Parameters / Member Variables
- `fd`: File descriptor to write zero data to
- `size`: Total number of zero bytes to write
- `offset`: File offset at which to begin writing zeros

## Dependencies
- Functions called/Symbols referenced:
  - pg_pwritev_with_retry: Underlying vectored write function with retry logic
  - PGIOAlignedBlock: Properly aligned buffer type for I/O operations
  - unconstify: Macro to cast away const qualifier from static buffer
  - PG_IOV_MAX: Maximum number of iovec entries per system call
  - BLCKSZ: PostgreSQL block size constant
  - iovec: Standard vectored I/O structure
- Called from (representative examples):
  - XLogFileInitInternal: Initializes transaction log files with zeros
  - FileZero: Generic file zeroing interface
  - dir_open_for_write: WAL backup utility file initialization

## Notes and Other Information
- The static zero buffer is shared across all calls to this function, making it thread-safe since it's read-only
- Uses PGIOAlignedBlock to ensure proper I/O alignment for optimal performance and direct I/O compatibility
- The function assert-checks that the total amount written equals the requested size on successful completion
- Each iovec entry can write up to BLCKSZ bytes, with smaller amounts for the final partial block
- This function is commonly used for initializing log files, extending files, and creating sparse file regions
- Part of PostgreSQL's cross-platform file I/O abstraction layer that handles platform-specific I/O requirements
# pg_pwrite_zeros

## Location
[src/common/file_utils.c:687-728](https://github.com/postgres/postgres/tree/92268b35d04c2de416279f187d12f264afa22614/src/common/file_utils.c#L687-L728)

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
  - [pg_pwritev_with_retry](pg_pwritev_with_retry.md): Underlying vectored write function with retry logic
  - PGIOAlignedBlock: Properly aligned buffer type for I/O operations
  - unconstify: Macro to cast away const qualifier from static buffer
  - PG_IOV_MAX: Maximum number of iovec entries per system call
  - BLCKSZ: PostgreSQL block size constant
  - [iovec](../i/iovec.md): Standard vectored I/O structure
- Called from (representative examples):
  - [XLogFileInitInternal](../X/XLogFileInitInternal.md): Initializes transaction log files with zeros
  - [FileZero](../F/FileZero.md): Generic file zeroing interface
  - [dir_open_for_write](../d/dir_open_for_write.md): WAL backup utility file initialization

## Notes and Other Information
- The static zero buffer is shared across all calls to this function, making it thread-safe since it's read-only
- Uses PGIOAlignedBlock to ensure proper I/O alignment for optimal performance and direct I/O compatibility
- The function assert-checks that the total amount written equals the requested size on successful completion
- Each iovec entry can write up to BLCKSZ bytes, with smaller amounts for the final partial block
- This function is commonly used for initializing log files, extending files, and creating sparse file regions
- Part of PostgreSQL's cross-platform file I/O abstraction layer that handles platform-specific I/O requirements

## Simplified Source

```c
// Simplified version of pg_pwrite_zeros
ssize_t pg_pwrite_zeros(int fd, size_t size, off_t offset) {
    static const PGIOAlignedBlock zbuffer = {{0}};  // Static zero-filled buffer
    void *zerobuf_addr = unconstify(PGIOAlignedBlock *, &zbuffer)->data;
    struct iovec iov[PG_IOV_MAX];
    size_t remaining_size = size;
    ssize_t total_written = 0;

    // Loop until all bytes are written
    while (remaining_size > 0) {
        int iovcnt = 0;

        // Fill iovec array with references to zero buffer
        for (; iovcnt < PG_IOV_MAX && remaining_size > 0; iovcnt++) {
            iov[iovcnt].iov_base = zerobuf_addr;

            // Use full block size or remaining bytes for last chunk
            size_t chunk_size = (remaining_size < BLCKSZ) ? remaining_size : BLCKSZ;
            iov[iovcnt].iov_len = chunk_size;
            remaining_size -= chunk_size;
        }

        // Perform vectored write with retry logic
        ssize_t written = pg_pwritev_with_retry(fd, iov, iovcnt, offset);

        if (written < 0)
            return written;  // Return error

        // Update position tracking
        offset += written;
        total_written += written;
    }

    return total_written;
}
```

Key simplifications made:
- Simplified variable declarations and initialization
- Added descriptive comments for main logic blocks
- Consolidated size calculation logic into a single line
- Removed detailed assertion checking for clarity
- Streamlined the vectored I/O setup process
- Maintained essential error handling and return logic
- Preserved the core algorithm of reusing a static zero buffer across multiple iovec entries